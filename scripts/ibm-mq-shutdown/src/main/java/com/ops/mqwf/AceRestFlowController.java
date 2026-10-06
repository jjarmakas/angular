package com.ops.mqwf;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Controls message flows through the integration node's administration REST API. Uses only the
 * JDK, so there is nothing extra to install on the server.
 * <p>
 * IIB v10 (ace.api=apiv1):<br>
 * GET {url}/apiv1/executiongroups/{server}/applications/{app}/messageflows/{flow}<br>
 * PUT the same path with ?action=stop or ?action=start. Never stopWithRestartExecutionGroup,
 * which forces the stop by restarting the whole integration server.
 * <p>
 * ACE v11/v12 (ace.api=apiv2):<br>
 * Integration node: {url}/apiv2/servers/{server}/applications/{app}/messageflows/{flow}<br>
 * Standalone integration server: {url}/apiv2/applications/{app}/messageflows/{flow}<br>
 * POST .../stop or .../start
 * <p>
 * Without an application the /applications/{app} segment is left out (independent flow).
 */
public final class AceRestFlowController implements FlowController {
    private static final Pattern IS_RUNNING = Pattern.compile("\"isRunning\"\\s*:\\s*(true|false)");
    private static final Pattern STATE = Pattern.compile("\"state\"\\s*:\\s*\"([A-Za-z]+)\"");

    private final Config.Ace settings;
    private final String authHeader;

    public AceRestFlowController(Config.Ace settings, AuditLog log) {
        this.settings = settings;
        if (settings.user != null) {
            String token = settings.user + ":" + settings.password;
            this.authHeader = "Basic " + Base64.getEncoder().encodeToString(token.getBytes(StandardCharsets.UTF_8));
            if (settings.url.startsWith("http://") && !isLocalhost(settings.url)) {
                log.warn("ACE credentials are sent over plain HTTP to %s; configure HTTPS for the admin REST API",
                        settings.url);
            }
        } else {
            this.authHeader = null;
        }
    }

    @Override
    public FlowState state(WorkflowDef wf) throws IOException {
        Response r = call("GET", flowPath(wf));
        if (r.code == 404) {
            throw new IOException("Message flow " + wf.flowLabel() + " not found (HTTP 404) at " + settings.url);
        }
        r.requireSuccess("read state of " + wf.flowLabel());
        return parseState(r.body, settings.apiV1);
    }

    @Override
    public void stop(WorkflowDef wf) throws IOException {
        action(wf, "stop").requireSuccess("stop " + wf.flowLabel());
    }

    @Override
    public void start(WorkflowDef wf) throws IOException {
        action(wf, "start").requireSuccess("start " + wf.flowLabel());
    }

    private Response action(WorkflowDef wf, String action) throws IOException {
        return settings.apiV1
                ? call("PUT", flowPath(wf) + "?action=" + action)
                : call("POST", flowPath(wf) + "/" + action);
    }

    /**
     * Reads the running state from the flow resource; anything unexpected is UNKNOWN (fail safe).
     * IIB v10 reports isRunning on the messageFlow object itself; ACE v11/v12 inside "active".
     */
    static FlowState parseState(String json, boolean apiV1) {
        int active = apiV1 ? -1 : json.indexOf("\"active\"");
        String scope = active >= 0 ? json.substring(active) : json;
        Matcher m = IS_RUNNING.matcher(scope);
        if (m.find()) {
            return Boolean.parseBoolean(m.group(1)) ? FlowState.RUNNING : FlowState.STOPPED;
        }
        m = STATE.matcher(scope);
        if (m.find()) {
            String s = m.group(1).toLowerCase();
            if (s.equals("started") || s.equals("running")) {
                return FlowState.RUNNING;
            }
            if (s.equals("stopped")) {
                return FlowState.STOPPED;
            }
        }
        return FlowState.UNKNOWN;
    }

    String flowPath(WorkflowDef wf) {
        StringBuilder p = new StringBuilder(settings.apiV1 ? "/apiv1" : "/apiv2");
        if (settings.apiV1) {
            p.append("/executiongroups/").append(segment(wf.server));
        } else if (settings.nodeMode) {
            p.append("/servers/").append(segment(wf.server));
        }
        if (wf.application != null) {
            p.append("/applications/").append(segment(wf.application));
        }
        return p.append("/messageflows/").append(segment(wf.flow)).toString();
    }

    private static String segment(String s) {
        try {
            return URLEncoder.encode(s, "UTF-8").replace("+", "%20");
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException(e);
        }
    }

    private Response call(String method, String path) throws IOException {
        URL url = new URL(settings.url.replaceAll("/+$", "") + path);
        HttpURLConnection c = (HttpURLConnection) url.openConnection();
        try {
            c.setRequestMethod(method);
            c.setInstanceFollowRedirects(false);
            c.setConnectTimeout(settings.httpTimeoutSeconds * 1000);
            c.setReadTimeout(settings.httpTimeoutSeconds * 1000);
            c.setRequestProperty("Accept", "application/json");
            if (authHeader != null) {
                c.setRequestProperty("Authorization", authHeader);
            }
            if (!"GET".equals(method)) {
                c.setDoOutput(true);
                c.setRequestProperty("Content-Type", "application/json");
                c.setFixedLengthStreamingMode(0);
                try (OutputStream out = c.getOutputStream()) {
                    out.flush();
                }
            }
            int code = c.getResponseCode();
            InputStream in = code >= 400 ? c.getErrorStream() : c.getInputStream();
            return new Response(method + " " + url, code, read(in));
        } finally {
            c.disconnect();
        }
    }

    private static String read(InputStream in) throws IOException {
        if (in == null) {
            return "";
        }
        try (InputStream is = in) {
            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            byte[] chunk = new byte[8192];
            int n;
            while ((n = is.read(chunk)) > 0 && buf.size() < 4 * 1024 * 1024) {
                buf.write(chunk, 0, n);
            }
            return new String(buf.toByteArray(), StandardCharsets.UTF_8);
        }
    }

    private static boolean isLocalhost(String url) {
        return url.matches("https?://(localhost|127\\.0\\.0\\.1|\\[::1\\])(:\\d+)?/?");
    }

    private static final class Response {
        final String request;
        final int code;
        final String body;

        Response(String request, int code, String body) {
            this.request = request;
            this.code = code;
            this.body = body;
        }

        void requireSuccess(String action) throws IOException {
            if (code < 200 || code > 299) {
                String snippet = body.length() > 500 ? body.substring(0, 500) + "..." : body;
                throw new IOException("Could not " + action + ": " + request + " returned HTTP " + code + " " + snippet);
            }
        }
    }
}
