# IBM MQ Queue Archiver

JavaCompute node code for IBM Integration Bus (Integration Toolkit 10.0.0.24,
IBM MQ 9.2.3.0). On each invocation it checks the current depth of a
configured list of queues and, for any queue whose depth exceeds its limit,
moves **all** of its messages into:

```
<archiveBasePath>/Archive_YYYYMMDD/<queueName>.txt
```

where `YYYYMMDD` is the date the archiving run happens, and `<queueName>.txt`
is appended to (not overwritten) if the same queue is archived more than
once on the same day.

## How it works

All the code lives in a single file,
`src/com/mqarchiver/QueueArchiverComputeNode.java`, containing three
classes:

- `QueueArchiverComputeNode` is the JavaCompute node entry point (the only
  public/top-level class, must match the file name). It loads
  configuration, runs the archiving pass, sends a success/failure log to
  the `log` terminal (see "Archiving log output" below), and passes the
  input message through to its `out` terminal unchanged (it does not act
  on message content).
- `ArchiverConfig` loads and validates `archiver.properties`.
- `QueueArchiver` does the actual work per queue:
  1. Opens the queue with local bindings (`MQQueueManager`/`MQQueue` from
     the MQ classes for Java) and reads `getCurrentDepth()`.
  2. If depth > limit, shells out to the **dmpmqmsg** utility shipped with
     IBM MQ using a destructive get (`-I`), which dumps every message on the
     queue to a temp file *and removes it from the queue as it does so* -
     this is what "moves" the messages. The temp file's contents are then
     appended to `<archiveBasePath>/Archive_YYYYMMDD/<queueName>.txt` and
     the temp file is deleted.
  3. One queue failing (bad name, MQ error, dmpmqmsg error) does not stop
     the remaining queues from being checked; it is recorded as a
     `FAILURE` line in the archiving log instead.

## Archiving log output

Every run produces one log line per configured queue (`OK` - below limit,
`SUCCESS` - archived, or `FAILURE` - error, with details), joined into a
single text message and propagated out the JavaCompute node's `log`
terminal as a BLOB. It is **not** just written to the broker/server log.

Wire that terminal to a **FileOutput** node so the lines land in
`<archiveBasePath>/Archive_YYYYMMDD/archiver.log` (appended, one file per
day, alongside that day's archived queue files):

```
TimeoutNotification ──> QueueArchiverComputeNode ─out─> (discard/log the output)
                                            └─log─> FileOutput
```

`QueueArchiverComputeNode` sets Local Environment
`Destination.File.Name`, `Destination.File.Directory` and
`Destination.File.Append` (`true`) on the message it sends to `log` before
propagating, so the FileOutput node should be left with its own
**File name**/**Directory name** properties blank (or "take from Local
Environment", depending on your toolkit version) so those values are used.
Verify this against your Integration Toolkit 10.0.0.24 FileOutput node
properties - the exact override behavior/checkbox has moved between
versions.

The `log` terminal isn't one of the JavaCompute node's default terminals;
add it via the node's properties in the Integration Toolkit ("Terminals"
tab / New Terminal, output, named `log`) after creating the node.

## Important: this node is timer-driven, not message-driven

Checking multiple queues' depths on a schedule isn't something a
JavaCompute node does on its own — it only runs when a message arrives at
its input terminal. Wire it behind a **TimeoutNotification** (or
TimeoutControl) input node configured for your desired polling interval
(e.g. every 5 minutes), not behind an MQInput node reading application
traffic:

```
TimeoutNotification ──> QueueArchiverComputeNode ──> (discard/log the output)
```

Attach a `failure` terminal / catch node per your normal error-handling
pattern if you want unexpected node-level errors (e.g. the configuration
file failing to load) surfaced beyond the broker log; per-queue
success/failure goes to the `log` terminal instead, as described above.

## Setup in the Integration Toolkit

1. In your message flow project, add a JavaCompute node, open its
   properties, and set the Java class to `com.mqarchiver.QueueArchiverComputeNode`.
   This generates a Java project with a stub source file.
2. Replace the generated stub with `src/com/mqarchiver/QueueArchiverComputeNode.java`
   from this repo, keeping the same package/folder. It contains all three
   classes (`QueueArchiverComputeNode`, `ArchiverConfig`, `QueueArchiver`)
   in one file.
3. Add an output terminal named `log` to the node (Terminals tab / New
   Terminal) and wire it to a FileOutput node - see "Archiving log output"
   above.
4. Add the MQ classes for Java jar to the Java project's build path if it
   isn't already resolved (`com.ibm.mq.allclient.jar`, found under your MQ
   9.2.3.0 install's `java/lib` folder, or already on the integration
   node's classpath).
5. Deploy `config/archiver.properties` (edited for your environment) to a
   path readable by the integration server's process owner, and point the
   node at it — see "Configuration file location" below.

## Configuration file location

The node resolves the properties file path, in order:

1. JVM system property `mqarchiver.configPath` (set via the integration
   server's `jvm.properties` / `-D` startup override).
2. Environment variable `MQARCHIVER_CONFIG_PATH`.
3. Default: `mqarchiver/archiver.properties`, relative to the integration
   server's working directory.

See `config/archiver.properties` for the file format (queue manager name,
queue list, default/per-queue message limits, archive base path, path to
`dmpmqmsg`).

## Prerequisites / things to verify in your environment

- **Local bindings**: the integration server must run on the same host as
  the queue manager (`qmgrName` in the config). This code does not do a
  client connection.
- **Authority**: the integration server's OS/MCA user needs `+inq` on each
  monitored queue (to read depth) and `+get`/`+browse` + `+dsp` as required
  by `dmpmqmsg` to dump and remove messages.
- **dmpmqmsg availability**: `dmpmqmsg` must be installed and runnable by
  the integration server process (it ships with the MQ client/server
  install). Either put it on the process's `PATH` or set `dmpmqmsgPath` in
  `archiver.properties` to its full path.
- **dmpmqmsg flags**: the command built in `QueueArchiver.runDmpmqmsg()` is
  `dmpmqmsg -m <qmgr> -I <queue> -f <file> -q`, where `-I` is a destructive
  get and `-f` writes the dump to a file. This flag set was confirmed
  against IBM's MQ 9.x documentation for `dmpmqmsg`, but IBM has tweaked
  utility flags between fix packs before. **Before relying on this in
  production, run `dmpmqmsg -?` on your MQ 9.2.3.0 install and confirm `-m`,
  `-I`, `-f` and `-q` behave as expected**, adjusting `runDmpmqmsg()` if
  your build differs.
- **Archive folder permissions**: the process owner needs write access to
  `archiveBasePath`; the code creates `Archive_YYYYMMDD` under it as needed
  (this now also holds `archiver.log`, written via the FileOutput node).
- **FileOutput node override behavior**: confirm your Integration Toolkit
  10.0.0.24 FileOutput node actually honors the `Destination.File.*` Local
  Environment values set by this node - see "Archiving log output" above.

## Testing outside the toolkit

`getCurrentDepth()` and `runDmpmqmsg()` can be exercised directly against a
test queue manager by writing a small `main()` that calls
`ArchiverConfig.load(...)` and `new QueueArchiver(config).archiveQueuesExceedingLimit()`
with `com.ibm.mq.allclient.jar` on the classpath, before wiring it into the
toolkit flow — useful for confirming the `dmpmqmsg` flags and file layout
against your actual MQ install before deploying.
