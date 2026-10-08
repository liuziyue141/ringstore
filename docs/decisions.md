# Design decisions

Each section is a choice we made, the simpler-looking alternative, and why we did not take it.

## Compute placement on the client, not with a lookup service

**Alternative:** a service that tells clients which backends own a bin.

**Why not:** every process already has the same config. Hashing locally gives the same answer everywhere with no extra service to keep alive or in sync. We use one ring position per backend to keep the code small; virtual nodes and load-balancing tuning are left out.

## Give each operation a random ID, not a hash of its value

**Alternative:** identify an entry by hashing its contents.

**Why not:** two real `append("paid")` calls are two operations and must both count, but one operation delivered twice must count once. A random ID created per call (and reused on that call's retries) tells these apart. A content hash would merge the two real appends. Order comes from `(timestamp, id)`.

## Recompute repair each pass, instead of saving progress

**Alternative:** the keeper records how far it got ("copied keys 1 to 500 to backend D") and resumes from there.

**Why not:** that record lives in the keeper's memory and disappears when the keeper crashes. The backends' logs already contain everything needed. Each pass merges them and copies only missing IDs, so redoing work is harmless, and any keeper can pick up after another. The cost is reading full logs every pass.

## Keep old copies and read from every backend

**Alternative:** read only from the current 3 owners and delete copies from old owners.

**Why not:** right after a failure, a new owner may still be empty. Reading only the owners would make data look missing until the keeper catches up. Reading every backend always finds the data. The cost is more read traffic and more than three copies of some keys, until a safe cleanup protocol exists.

## Remove list items by ID, not by deleting entries

**Alternative:** physically delete the matching appends from the log.

**Why not:** an old backend that still has the append would copy it back during repair. Instead, a remove names the append IDs it saw. A late copy of one of those appends stays hidden, and a new equal append (new ID) survives. Single values use a delete marker for the same reason. Cleaning these up would require knowing no old copy can come back.

## Keep the clock outside user keys

**Alternative:** store the clock in a reserved bin or key.

**Why not:** that would take over a name users might want, and mix system data with user data. Instead, each backend keeps its counter, restart ID, and highest clock value as process state, exposed through a backend-only service. Fixed slots keep backends from handing out the same value. This relies on fixed membership and on some backend surviving to remember the highest value.

## Test by killing real processes

**Alternative:** simulate a crash by cancelling a task inside one test process.

**Why not:** a cancelled task can leave sockets, background tasks, or shared memory alive, which hides real bugs. The fault tests start each backend and keeper as a separate process and kill it. A backend that pauses after one copied entry lets a test kill the keeper at a known point mid-copy. Tests pick free ports and poll for results instead of sleeping for fixed times.
