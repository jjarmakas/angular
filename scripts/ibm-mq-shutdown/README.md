# mqwf: safe shutdown of individual MQ/IIB workflows

`mqwf` shuts down one workflow at a time on IBM MQ 9.2.3 on Windows and starts it again later. A workflow is one IBM Integration Bus v10 (IIB 10.0.0.24) message flow plus the local queue it uses. App Connect Enterprise v11/v12 is supported too (`ace.api=apiv2`). The tool only finishes a shutdown when the workflow's queue is truly empty, and no database row or message is lost along the way.

| Type       | Flow                                          | Queue role                                   |
|------------|-----------------------------------------------|----------------------------------------------|
| `DB_TO_MQ` | DatabaseInput → MQOutput → local queue        | flow **produces**; a remote client consumes  |
| `MQ_TO_DB` | local queue → MQInput → Database node         | a remote client produces; flow **consumes**  |

---

## 1. Why Java 1.8 and not PowerShell

Java 1.8 was chosen. The main reason is how safety-critical state gets read: Java can read queue state through the MQ API, while PowerShell would have to read it from command text.

| Concern | Java 1.8 | PowerShell |
|---|---|---|
| Reading queue state (`CURDEPTH`, `UNCOM`, `IPPROCS`, `PUT`) | **PCF** via IBM MQ classes for Java (`PCFMessageAgent`). Values arrive as typed integers with MQ reason codes. | No supported PCF API. You have to run `runmqsc` and parse `DISPLAY QSTATUS` text with regexes. That text depends on the locale and the MQ version, and errors show up as text, not as reason codes. |
| Supported by IBM | MQ classes for Java are a documented, supported admin API. They ship with MQ 9.2 (`java\lib`). | The MQ .NET classes have no supported PCF support. |
| Runtime already on the server | MQ 9.2 installs a Java 8 runtime, and the tool runs on it. The Java level of IIB itself does not matter. Nothing to install. | Built into Windows. |
| Error handling | Checked exceptions. Every failure path has to be handled, and the compiler enforces it. | Native commands fail through `$LASTEXITCODE` and stderr text. Under `$ErrorActionPreference` it is easy to carry on after a failed step without noticing. |
| Testability | The safety logic (ordering, rollback, `UNCOM` handling) is unit-tested with fakes. There are 29 tests in `src/test`. | Possible with Pester, but the logic is tied to text parsing. |
| Portability | The same jar runs if MQ/IIB ever moves to Linux. | Windows-specific. |

PowerShell's real advantages are that it needs no compile step and admins can read it easily. Those are outweighed here, because the one decision that must never be wrong is "is this queue really empty, including in-flight units of work?". Java gets that answer from a structured API instead of from parsed text.

---

## 2. How a shutdown works

### Why "empty" means `CURDEPTH = 0` **and** `UNCOM = 0`

An `MQGET` under syncpoint lowers `CURDEPTH` immediately, before the unit of work commits. So while the flow is still writing a message to the database, the queue already shows `CURDEPTH = 0`. If that database insert then fails, the get backs out and the message reappears.

Stopping on `CURDEPTH = 0` alone is therefore unsafe. `mqwf` also requires `UNCOM = 0` (no uncommitted puts or gets), and it requires both on **N consecutive checks** (`drain.stablePolls`, at least 2). A message that backs out resets the count.

### `MQ_TO_DB` (remote client → queue → flow → database)

1. **Pre-flight (read only).** The queue must exist and be a local queue. The flow state must be known. The queue must not be `GET(DISABLED)` while holding messages. If the flow is stopped while messages are waiting, the run is refused, because nothing would consume them.
2. **Drain.** The flow keeps running until `CURDEPTH = 0` and `UNCOM = 0` hold on N consecutive checks. The queue stays `PUT(ENABLED)` and remote clients can keep sending, so this amounts to a **quiet period** of `drain.stablePolls × drain.pollIntervalSeconds` seconds with no new input. If clients send continuously, the drain times out and the flow is left running. If messages are waiting but nothing has the queue open (`IPPROCS = 0`) for `drain.stallPolls` checks, the run aborts early.
3. **Stop the flow normally.** This is never a forced stop. No unit of work is in flight at this point. The tool waits until IIB reports the flow as stopped.
4. **Verify** that the queue is still empty. A message a client puts after the stop is safe on the queue (if it is persistent), but the queue is no longer empty. In that case the tool restarts the flow so it processes the message, and exits with code 5 (shutdown not completed).
5. **Backout queue check.** If `BOQNAME` grew during the drain, the run reports exit code 8. Those messages are not lost, but someone needs to look at them.

For a reliable shutdown, ask the owners of the remote clients to pause sending first. The tool cannot stop them, because it never changes queue attributes.

### `DB_TO_MQ` (database → flow → queue → remote client)

1. **Pre-flight** (same checks as above). The tool warns if messages are waiting and no remote client is connected.
2. **Stop the producer flow normally.** The DatabaseInput unit of work that is in flight completes as a whole. Rows the flow has not read yet stay in the database and are picked up on the next start. No rows are lost.
3. **Drain.** The tool waits for the remote client to consume everything (`CURDEPTH = 0` and `UNCOM = 0`, N times in a row). Stall detection is off for this type, because a remote client may only connect from time to time.
### Failure handling and rollback

The only thing the tool ever changes is whether the message flow is running. If anything fails after it stopped the flow (timeout, REST or MQ error, failed verification), it restarts the flow, but only if this run stopped it and IIB now reports it as stopped.

If the rollback itself fails, the exit code is `10` and the audit log says exactly what is still in effect and how to fix it. `--no-rollback` leaves the flow stopped instead. Running `shutdown` again resumes from where it stopped (each step is idempotent).

`start` reverses a shutdown by starting the flow. It never changes the queue's `PUT` attribute. If it finds an `MQ_TO_DB` queue set to `PUT(DISABLED)` by someone else, it only warns.

### Several workflows in one run

The `workflows` property lists every workflow, in processing order. Without workflow names, `status`, `shutdown` and `start` process all of them in that order. With names (`mqwf shutdown A C`), only those are processed, in the order given.

The workflows are independent, so the tool handles them **one at a time**, each completely on its own:

1. **Confirm once.** Before anything is touched, the operator sees the list and types the action and the count, for example `SHUTDOWN 3`. For a single workflow it is still its name.
2. **For each workflow in turn:** take its lock, run its pre-flight checks and log its plan, run its full sequence (drain, stop, verify for a shutdown; start for a start-up), then release its lock. Only then does the next workflow begin.
3. **A problem stays with its workflow.** If a workflow is locked by another run, fails pre-flight, or fails during execution (and is rolled back as usual), the tool records that and moves on to the next workflow. Other workflows are never blocked, skipped or undone because of it.
4. **Summary.** The log ends with one line per workflow (OK, NOTHING TO DO or FAILED). The exit code is the most severe result among the workflows. For example, `10` (rollback failed) outranks `3` (drain not completed), which outranks `8` (warnings).

Because each workflow is checked only when its turn comes, `--dry-run` is the way to preview every workflow's plan before a real run.

---

## 3. Production safety features

- **Read-only on MQ.** The only MQ operations are `INQUIRE_Q` and `INQUIRE_Q_STATUS`. The tool never changes queue attributes, and never gets, browses, clears, deletes or force-stops anything.
- **`--dry-run`** runs every pre-flight check and prints the exact plan without changing anything.
- **Confirmation.** The operator has to type the workflow name, or for several workflows the action and the count (e.g. `SHUTDOWN 3`). If no console is attached, the tool refuses to run unless `--yes` is given explicitly.
- **One run per workflow.** An OS file lock (`lock.dir`) prevents two runs on the same workflow at once. In a multi-workflow run each workflow is locked only while it is being processed. The OS releases the lock when the process dies, so no stale locks are left behind.
- **Strict configuration.** Wildcard or invalid queue names are rejected. Two workflows that share a queue or a flow are rejected. Numbers are range-checked. Passwords are only read from environment variables, and plain-text passwords in the file are rejected.
- **Fail-safe defaults.** An unknown flow state, an unexpected REST response, a missing PCF parameter or a non-local queue all stop the run before any change.
- **Bounded waits.** Every wait has a timeout: drain, flow state and HTTP.
- **Audit trail.** Every check, decision and change goes to a flushed, append-only daily file `logs/mqwf-audit-YYYYMMDD.log`, with the user, host and options used.
- **Ctrl+C or service stop.** A shutdown hook logs which phase was interrupted and which changes are in effect.
- **Connection resilience.** A broken MQ connection (reason 2009) gets one reconnect and retry. All commands are idempotent, so retrying is safe.
- **Distinct exit codes** for schedulers and monitoring (see below).

---

## 4. Build, install, configure

**Build**, either way:

```bat
rem Without Maven, using any Java 8 JDK (on a build machine; only the jar goes to the server)
set JAVA_HOME=C:Javajdk8
build.cmd

rem Or with Maven
mvn package
```

**Install** on the MQ server by copying `mqwf.jar` and `mqwf.cmd` to one folder. Then copy `config\mqwf.properties.example` to `mqwf.properties` in that same folder and edit it. At runtime the tool uses the `com.ibm.mq.allclient.jar` of the installed MQ, so client and server levels always match.

**Permissions** (least privilege, for a dedicated user such as `mqwfadm`):

```bat
setmqaut -m QM1 -t qmgr -p mqwfadm +connect +inq +dsp
setmqaut -m QM1 -n SYSTEM.ADMIN.COMMAND.QUEUE -t queue -p mqwfadm +put
setmqaut -m QM1 -n SYSTEM.DEFAULT.MODEL.QUEUE -t queue -p mqwfadm +dsp +get
setmqaut -m QM1 -n "APP.**" -t queue -p mqwfadm +inq +dsp
```

The MQ command server has to be running (`strmqcsv QM1`). The integration node web administration interface must be enabled (it is by default on port 4414). If administration security is on, the user needs read access to the integration server and permission to start and stop its message flows (`mqsichangefileauth`). Use HTTPS for the web admin port when the tool runs on another host.

### IIB 10 vs. ACE 11/12

Set `ace.api` to match the runtime. The Integration Toolkit version does not matter, because the tool talks only to the running integration node.

| | `ace.api=apiv1` (IIB 10.0.0.x) | `ace.api=apiv2` (ACE 11/12) |
|---|---|---|
| Flow resource | `/apiv1/executiongroups/{server}/applications/{app}/messageflows/{flow}` | `/apiv2/servers/{server}/applications/{app}/messageflows/{flow}` |
| Stop / start | `PUT ...?action=stop` / `?action=start` | `POST .../stop` / `.../start` |
| Running state | `isRunning` on the flow object | `active.isRunning` |
| `ace.mode` | `node` only | `node` or `server` |

`workflow.<name>.server` is the integration server (execution group) name. For a flow deployed outside an application, leave `workflow.<name>.application` empty. The tool only ever sends the normal `stop` action. It never uses `stopWithRestartExecutionGroup`, which forces the stop by restarting the whole integration server and would interrupt every other flow on it.

---

## 5. Usage

```bat
mqwf list
mqwf status
mqwf shutdown --dry-run
mqwf shutdown
mqwf shutdown --yes
mqwf start
mqwf status   PAYMENTS_IMPORT
mqwf shutdown PAYMENTS_IMPORT ORDERS_EXPORT
mqwf shutdown ORDERS_EXPORT --yes --timeout 3600
mqwf start    PAYMENTS_IMPORT
```

| Exit | Meaning | Changes in effect |
|---|---|---|
| 0 | Completed and verified | as requested |
| 1 | Usage or configuration error | none |
| 2 | Pre-flight check failed | none |
| 3 | Queue did not drain (timeout, stall, or no quiet period) | flow running (restarted if needed, unless `--no-rollback`) |
| 4 | Flow did not reach the expected state | flow restarted if needed |
| 5 | Queue not empty after the flow stopped (e.g. a client put a message after the stop) | flow restarted |
| 6 | A workflow is locked by another run (other workflows are still processed) | none for that workflow |
| 7 | Not confirmed | none |
| 8 | Completed, but messages went to the backout queue | as requested |
| 9 | Unexpected MQ, REST or I/O error | rolled back where possible |
| 10 | **Rollback failed: manual action required** (see audit log) | partial |

---

## 6. Flow settings that make "no database loss" hold

A graceful stop completes the current unit of work. The flows themselves must make that unit of work atomic:

- **MQInput / MQOutput:** `Transaction mode = Yes`, and use **persistent** messages (`DEFPSIST(YES)` or the MQOutput persistence setting). Non-persistent messages do not survive a queue manager restart.
- **Database / DatabaseInput nodes:** `Transaction = Automatic`, so the database work commits or rolls back together with the flow.
- **Database and MQ in one atomic unit:** set `coordinatedTransaction=yes` with an XA-configured data source. Without XA, a crash between the database commit and the MQ commit can cause a *duplicate* (redelivery), never a loss. In that case make the inserts idempotent.
- **`BOQNAME` / `BOTHRESH`** on `MQ_TO_DB` queues: a poison message then goes to the backout queue instead of blocking the drain until the timeout.

---

## 7. Before first production use

- Run `mqwf status` and `shutdown --dry-run` against every workflow in a test environment. They confirm the REST paths, the flow-state parsing and the MQ and IIB permissions.
- The tool assumes each configured queue is used only by its own workflow. The configuration enforces this between workflows, but it cannot know about outside applications.
- For `MQ_TO_DB`, agree a window with the owners of the remote clients in which they pause sending. While the flow is stopped their messages wait on the queue, so they must be persistent to survive a queue manager restart.
