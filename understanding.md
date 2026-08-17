TRANSIT SYSTEM — TECHNICAL UNDERSTANDING GUIDE

GOAL
The coding agent is implementing most of this project. My goal is to understand the system deeply enough that I could:
- explain the architecture from memory
- justify major design decisions
- understand what the important code is doing
- identify likely failure modes
- discuss reasonable alternatives
- modify/debug the system intelligently
- defend the project in a technical interview

When explaining something to me, prioritize engineering intuition over textbook definitions.

For important concepts, explain:
1. What is it?
2. What problem does it solve?
3. Where does it appear in MY system?
4. What would happen if we didn't have it?
5. What alternative could we use?
6. Why did we choose this implementation?
7. At what scale/condition would this decision stop making sense?

Do not make me manually write documentation as part of the learning process. Generate documentation/notes for me when useful, but ask me questions periodically to verify that I actually understand the system.


============================================================
A. SYSTEM OVERVIEW
============================================================

[ ] Can I draw the entire architecture from memory?

[ ] What happens, step-by-step, from:
    transit agency publishes an update
    →
    our system receives it
    →
    we store it
    →
    someone asks when their bus is coming
    →
    our API returns an answer?

[ ] What are the major components of our system?

[ ] What responsibility does each component have?

[ ] Where are the boundaries between components?

[ ] Which components know about the specific transit agency?

[ ] Which components are agency-agnostic?

[ ] Where does data enter our system?

[ ] Where does data leave our system?

[ ] Where does state live?

[ ] Which components are stateful?

[ ] Which components are stateless?

[ ] If the entire application restarted right now, what would we lose?

[ ] What is currently the biggest single point of failure?

[ ] What is currently the bottleneck?
    The bottleneck is the composite key between the RT update data and the trip schedule backbone which takes compute time and storage to keep an updated log of what is stale trip data and what is fresh.
[ ] How would we MEASURE whether something is actually a bottleneck rather than guessing?
    Whether there is measureable slow down in ingestion speed

============================================================
B. GTFS / TRANSIT DOMAIN
============================================================

[ ] What is GTFS?

[ ] What problem was GTFS designed to solve?

[ ] What is the difference between static GTFS and GTFS-Realtime?
    The static GTFS is the planned trip schedule with the routes, arrival times, and vehicle positions that *should* occur while the GTFS-Realtime is the batched updates for the actual progress on those arrival times, stop order, and positions. the static GTFS is a text schedule while the realtime is a stream.
[ ] Why do we need both?
    We need both to collect data on the approximate difference between the scheduled and actual arrival times to create a model down the line that can reliably predict when a bus will actually arrive.
[ ] What is an agency?

[ ] What is a route?

[ ] What is a trip?

[ ] What is a stop?

[ ] What is a stop_time?

[ ] What is a shape?

[ ] What is a service/calendar?

[ ] Why is a route NOT the same thing as a trip?
    A route is the full path a vehicle may take while a trip may be a subset of that route between 2 or more stops.
[ ] Explain the relationship:

    agency
       ↓
    route
       ↓
    trip
       ↓
    stop_times
       ↓
    stops

[ ] What is a GTFS VehiclePosition?

[ ] What is a GTFS TripUpdate?

[ ] What's the difference between:
    "the bus is physically here"
    and
    "the bus is predicted to arrive at 3:42"?

[ ] How do we connect a realtime VehiclePosition to the corresponding static GTFS information?
    Through a composite key of the trip ID, the route ID, and the scheduled time.
[ ] Which identifiers are stable?

[ ] Which identifiers should we NOT assume are globally unique?

[ ] What happens when a trip is canceled?

[ ] What happens when an unscheduled trip appears?

[ ] What happens when a vehicle disappears from the realtime feed?

[ ] What assumptions can we safely make about the ordering of realtime updates?

[ ] What does "stale" transit information mean?
    That its more than 30 seconds old (the time between updates to the RT stream).
[ ] How does our system determine freshness?


============================================================
C. DATA INGESTION
============================================================

[ ] What exactly is our ingestion process doing?

[ ] Why are we polling rather than receiving events pushed to us?

[ ] How frequently are we polling?

[ ] Why did we choose that frequency?

[ ] What would change if we polled every:
    - 1 second?
    - 10 seconds?
    - 30 seconds?
    - 5 minutes?

[ ] What does one polling cycle look like?

[ ] What happens if a polling cycle fails?

[ ] What happens if the transit server takes 20 seconds to respond?

[ ] What happens if it never responds?

[ ] Do we use a timeout?

[ ] Why?

[ ] Should we retry?

[ ] If so, how many times?

[ ] What is exponential backoff?

[ ] Would it be useful here?

[ ] What happens if two polling cycles overlap?

[ ] Could that happen in our implementation?

[ ] What does idempotency mean?

[ ] Is our ingestion idempotent?

[ ] Should it be?

[ ] What happens if we ingest the exact same observation twice?

[ ] How do we identify duplicates?

[ ] What happens if events arrive out of chronological order?

[ ] Do we validate incoming data?

[ ] Which assumptions about the incoming data are dangerous?


============================================================
D. HTTP / NETWORKING
============================================================

[ ] When our Python code makes an HTTP request, what actually happens?

Explain:

    Python application
         ↓
    DNS
         ↓
    TCP
         ↓
    TLS
         ↓
    HTTP
         ↓
    remote server
         ↓
    response
         ↓
    bytes
         ↓
    decoding
         ↓
    Python objects

[ ] What is DNS doing?

[ ] What is TCP doing?

[ ] What is TLS doing?

[ ] What is HTTP doing?

[ ] What is a socket?

[ ] What is latency?

[ ] What contributes to the latency of our GTFS request?

[ ] What is a timeout?

[ ] What is connection pooling?

[ ] Are we reusing HTTP connections?

[ ] Why might that matter?

[ ] What is serialization?

[ ] Where does serialization/deserialization occur in our system?


============================================================
E. PROTOCOL BUFFERS
============================================================

[ ] What is protobuf?

[ ] Why does GTFS-Realtime use protobuf instead of JSON?

[ ] What does the .proto schema define?

[ ] What does our Python protobuf library actually do?

[ ] What happens between receiving the HTTP response bytes and accessing something like:

    vehicle.position.latitude

[ ] What are the benefits of binary serialization?

[ ] What are the disadvantages?

[ ] How would our implementation differ if GTFS-RT were JSON?


============================================================
F. OUR INTERNAL DATA MODEL
============================================================

[ ] Why don't we simply store whatever format each transit agency gives us?

[ ] What does "normalization" mean in OUR application?

[ ] What is our internal representation of:

    Agency
    Route
    Trip
    Stop
    Vehicle
    VehicleObservation

[ ] Why do we have a VehicleObservation instead of simply Vehicle?

[ ] What's the difference between:

    current state

    and

    historical observations?

[ ] Why are we preserving historical observations?

[ ] What is observed_at?

[ ] What is ingested_at?

[ ] Why are those different timestamps?

[ ] Give me a concrete scenario where:

    observed_at != ingested_at

[ ] Why does that distinction matter?

[ ] If we add SF Muni tomorrow, which parts of our internal model change?

[ ] Which parts should NOT change?

[ ] Where have we created abstractions?

[ ] What problem does each abstraction solve?

[ ] Have we created any abstractions that may be premature?


============================================================
G. DATABASES / POSTGRES
============================================================

[ ] Why are we using Postgres?

[ ] What alternatives did we have?

[ ] Why not just store everything in:
    - JSON files?
    - CSV?
    - SQLite?
    - an in-memory Python dictionary?

[ ] What does Postgres give us?

[ ] What is a relational database?

[ ] What is a table?

[ ] What is a row?

[ ] What is a primary key?

[ ] What is a foreign key?

[ ] What is a uniqueness constraint?

[ ] Where are we using each?

[ ] Why isn't vehicle_id necessarily enough to uniquely identify a VehicleObservation?

[ ] What is a database index?

[ ] Conceptually, how does an index make a query faster?

[ ] What would happen if VehicleObservation eventually had 100 million rows?

[ ] What query patterns do we currently have?

[ ] Which columns should be indexed based on those query patterns?

[ ] Why shouldn't we index every column?

[ ] Explain what Postgres approximately needs to do for:

    SELECT *
    FROM vehicle_observations
    WHERE vehicle_id = ?
    ORDER BY observed_at DESC;

[ ] How would that differ with and without an appropriate index?

[ ] What is a query plan?

[ ] How can we inspect one?

[ ] What is a transaction?

[ ] What does ACID mean?

[ ] Which parts of ACID actually matter for our application?

[ ] What happens if ingestion crashes halfway through inserting a batch?

[ ] Do we want the entire batch to succeed/fail atomically?

[ ] What is a database connection?

[ ] What is a connection pool?

[ ] Why shouldn't every API request create a completely new database connection?


============================================================
H. STORAGE / SCALE
============================================================

[ ] Approximately how large is one VehicleObservation?

[ ] How many observations are we generating per hour?

[ ] Per day?

[ ] Per month?

[ ] Estimate our storage growth.

[ ] What happens if we expand from:
    5 routes
    →
    all UCLA-area routes
    →
    LA
    →
    LA + SF?

[ ] Which component hits a limit first?

[ ] At what scale might our current database design become problematic?

[ ] Would we keep every observation forever?

[ ] Could observations eventually be compressed/downsampled?

[ ] What is partitioning?

[ ] At what scale might partitioning help?

[ ] What is the difference between scaling vertically and horizontally?


============================================================
I. FASTAPI / BACKEND
============================================================

[ ] Why are we using FastAPI?

[ ] What problem does FastAPI solve?

[ ] What would implementing the backend without FastAPI require us to do ourselves?

[ ] What happens when:

    GET /stops/{stop_id}/arrivals

    reaches our server?

Walk through every major step.

[ ] What process is actually running FastAPI?

[ ] What is an application server?

[ ] What is a request?

[ ] What is a response?

[ ] What is middleware?

[ ] Are we using any?

[ ] What is dependency injection in FastAPI?

[ ] Are we using it?

[ ] Why?

[ ] What determines our API response schema?

[ ] What happens when the database is unavailable?

[ ] What HTTP status should we return for different failures?

[ ] Which errors should users see?

[ ] Which errors should only appear in logs?


============================================================
J. CONCURRENCY / ASYNC
============================================================

[ ] What is concurrency?

[ ] What is parallelism?

[ ] What is the difference?

[ ] What is synchronous execution?

[ ] What is asynchronous execution?

[ ] What does async/await actually mean in Python?

[ ] Why is calling a remote transit API I/O-bound?

[ ] Why is waiting on Postgres potentially I/O-bound?

[ ] Why can async help with I/O?

[ ] When does async NOT make code faster?

[ ] What happens while our program is awaiting a network response?

[ ] What is an event loop?

[ ] What is a process?

[ ] What is a thread?

[ ] How are:
    processes
    threads
    async tasks
    different?

[ ] What happens if 100 people call our API simultaneously?

[ ] Could two requests interfere with each other?

[ ] Where could race conditions occur?

[ ] What is a race condition?


============================================================
K. CODEBASE STRUCTURE
============================================================

[ ] Walk me through the repository directory-by-directory.

[ ] What responsibility does each file have?

[ ] Why is the code separated this way?

[ ] What is the application's entry point?

[ ] Trace execution from startup.

[ ] Which code:
    - fetches GTFS?
    - parses GTFS?
    - normalizes data?
    - talks to Postgres?
    - contains domain logic?
    - exposes API endpoints?
    - renders the UI?

[ ] What is the dependency direction between these modules?

[ ] Is business/domain logic mixed into API routes?

[ ] Is database logic mixed into GTFS parsing?

[ ] If so, should it be?

[ ] What does "separation of concerns" mean?

[ ] Where does our codebase demonstrate it?

[ ] Where does it violate it?

[ ] What code would I need to modify to add Muni?

[ ] What code would I need to modify to change Postgres?

[ ] What code would I need to modify to add an ML ETA model?

[ ] If those changes require touching unrelated parts of the codebase, what does that tell us about our architecture?


============================================================
L. TESTING
============================================================

[ ] What are we testing?

[ ] Why are those things worth testing?

[ ] What's the difference between:
    unit tests
    integration tests
    end-to-end tests?

[ ] Give me an example of each for this system.

[ ] What behavior would be dangerous if silently incorrect?

[ ] How do we test missing GTFS fields?

[ ] How do we test stale data?

[ ] How do we test duplicate events?

[ ] How do we test the database?

[ ] Should tests call the real transit API?

[ ] Why or why not?

[ ] What is mocking?

[ ] When is mocking useful?

[ ] When can excessive mocking make tests meaningless?


============================================================
M. OBSERVABILITY / RELIABILITY
============================================================

[ ] What's the difference between:
    logs
    metrics
    traces?

[ ] Which are we currently using?

[ ] What should we log during ingestion?

[ ] What should we measure?

[ ] How would I know if the transit feed stopped updating?

[ ] How would I know if ingestion crashed overnight?

[ ] How would I know if API latency suddenly increased?

[ ] What are:
    P50
    P95
    P99
    latency?

[ ] Why isn't average latency sufficient?

[ ] What would "reliability" mean for this particular application?

[ ] What failure modes are acceptable for a personal MVP?

[ ] Which would become unacceptable if students actually depended on it?


============================================================
N. DOCKER / DEPLOYMENT
============================================================

[ ] What is Docker?

[ ] What problem is it solving for us?

[ ] What exactly is inside our container?

[ ] What is NOT inside the container?

[ ] What is an image?

[ ] What is a container?

[ ] What's the difference?

[ ] What does our Dockerfile do line-by-line?

[ ] Why does containerization improve reproducibility?

[ ] What happens when our application is deployed?

[ ] Where is the process physically running?

[ ] What happens when that machine/process restarts?

[ ] How does our deployed application reach Postgres?

[ ] Where do secrets/API credentials live?

[ ] Why shouldn't secrets be committed to Git?


============================================================
O. ARCHITECTURE TRADEOFFS
============================================================

For EVERY major technology we introduce, ask:

[ ] What concrete problem does this solve?

[ ] Do we currently have that problem?

[ ] What's the simplest alternative?

[ ] What additional complexity does this technology introduce?

[ ] What measurement would tell us we need it?

Specifically:

[ ] Why is this currently a monolith rather than microservices?

[ ] What specific problem would justify splitting a service out?

[ ] What is a queue?

[ ] What specific problem would justify adding one?

[ ] What is Redis?

[ ] What specific problem might justify adding it?

[ ] What is Kafka?

[ ] What specific problem might justify adding it?

[ ] Why would Kafka probably make our system worse RIGHT NOW?

[ ] What is caching?

[ ] What data in our system might eventually be worth caching?

[ ] What's the danger of caching?

[ ] What is horizontal scaling?

[ ] What would need to happen before we horizontally scale the API?

[ ] What would need to happen before we horizontally scale ingestion?

[ ] How would multiple ingestion workers create new problems?

[ ] What does "distributed system" actually mean?

[ ] At what point would our project become one?


============================================================
P. PERFORMANCE
============================================================

[ ] What does "performance" mean for our system?

[ ] What should we measure before optimizing?

[ ] What's our current:
    API P50 latency?
    P95?
    P99?

[ ] How long does a GTFS fetch take?

[ ] How long does protobuf decoding take?

[ ] How long does normalization take?

[ ] How long do DB inserts take?

[ ] How long does an arrival query take?

[ ] Which is currently slowest?

[ ] WHY is it slow?

[ ] What is profiling?

[ ] How could we profile this system?

[ ] What is throughput?

[ ] What's the difference between latency and throughput?

[ ] How could we load-test our API?

[ ] What would we learn by replaying historical GTFS data at 10x/100x/1000x real speed?


============================================================
Q. ADDING MACHINE LEARNING LATER
============================================================

DO NOT implement this until we have enough historical data.

Before building an ETA model:

[ ] What exactly constitutes one training observation?

[ ] What exactly is our target variable?

[ ] What does "prediction time" mean?

[ ] What information existed at prediction time?

[ ] Which features might accidentally contain future information?

[ ] What is data leakage?

[ ] Why might randomly splitting individual vehicle observations into train/test sets cause leakage?

[ ] Why might chronological splitting be better?

[ ] What's our simplest baseline?

[ ] What's our historical-average baseline?

[ ] What's our existing product baseline?

[ ] Can we beat the published schedule?

[ ] Can we beat the agency's own realtime ETA?

[ ] Why is beating an ML baseline insufficient if we can't beat what riders already have?

[ ] What metric should we optimize?

[ ] What does MAE measure?

[ ] Is predicting 5 minutes early equally harmful to a rider as predicting 5 minutes late?

[ ] Should our evaluation reflect that?

[ ] How should accuracy change based on prediction horizon?

[ ] Should one model serve every route?

[ ] Should one model serve every agency?

[ ] Should buses and trains share a model?

[ ] How would we TEST these choices instead of guessing?

[ ] What is an offline feature?

[ ] What is an online feature?

[ ] What is training-serving skew?

[ ] How could the same feature accidentally be computed differently during training and serving?

[ ] What is model drift?

[ ] What is data drift?

[ ] How could we distinguish:
    model degradation
    data drift
    upstream feed changes
    software bugs?

[ ] How would model versioning work?

[ ] How would we deploy a new model safely?

[ ] What is shadow deployment?

[ ] Could we run our model alongside the agency ETA without exposing it to users?

[ ] How would we compare them?

[ ] How could we return an ETA uncertainty interval rather than just:
    "8 minutes"?

[ ] How would we determine whether that interval is actually calibrated?


============================================================
R. MODEL SERVING
============================================================

Once ML exists:

[ ] Where should the trained model live?

[ ] When is it loaded into memory?

[ ] Should we load it for every request?

[ ] Why/why not?

[ ] What happens when an inference request arrives?

Trace:

    API request
       ↓
    feature retrieval
       ↓
    preprocessing
       ↓
    model inference
       ↓
    postprocessing
       ↓
    response

[ ] Which step dominates latency?

[ ] How do we know?

[ ] What happens with 100 concurrent predictions?

[ ] What happens with 10,000?

[ ] At what point might caching help?

[ ] At what point might batching help?

[ ] When would separating model serving from the main API make sense?

[ ] What would we gain?

[ ] What complexity would we introduce?


============================================================
S. MULTI-AGENCY EXPANSION
============================================================

When we eventually go:

UCLA
→ LA
→ SF

ask:

[ ] What assumptions did we accidentally make about our first agency?

[ ] Which broke when adding another agency?

[ ] Are IDs namespaced by agency?

[ ] Are route structures equivalent?

[ ] Are update frequencies equivalent?

[ ] Is data quality equivalent?

[ ] Are buses and rail represented similarly enough for our abstractions?

[ ] Which code required changes?

[ ] Which code didn't?

[ ] Does that suggest our abstraction was good?

[ ] Did we over-generalize anything before we needed to?

[ ] Should ingestion use adapters/connectors for each agency?

[ ] What common interface should those adapters expose?


============================================================
T. ENGINEERING INTUITION — ASK ME THESE FREQUENTLY
============================================================

Instead of asking me only "what does X mean?", periodically give me scenarios like:

1. The GTFS server starts returning the same timestamp for 10 minutes.
   What should our system do?

2. Postgres suddenly takes 2 seconds per query.
   How would I investigate?

3. We start receiving duplicate vehicle observations.
   Where could duplication originate?

4. Our API becomes slow at 500 concurrent users.
   What do I measure FIRST?

5. An ingestion worker crashes after processing 80% of a feed.
   What state are we left in?

6. We add Muni and our existing parser breaks.
   What does that tell us about our abstraction?

7. Historical observations reach 100M rows.
   What problems might appear?

8. Our ETA model performs great offline and terribly in production.
   What are plausible causes?

9. Our model's inference takes 10ms but the endpoint takes 600ms.
   Where should we investigate?

10. Someone suggests adding Redis.
    What evidence would I ask them for?

11. Someone suggests Kafka.
    What requirement would justify it?

12. Someone suggests microservices.
    What problem are they actually solving?

13. We can reduce latency from 80ms to 40ms but double infrastructure complexity.
    Is it worth it?

14. One agency updates every 10 seconds and another every 60 seconds.
    How should our system handle freshness?

15. A bus's observations arrive in this order:

    3:01:10
    3:01:30
    3:01:20

    What should happen?

16. Apple/agency ETA says 4 minutes.
    Our model says 7 minutes.
    Actual arrival is 8 minutes.

    What information should we record?

17. Our model has lower MAE overall but is worse during rush hour.
    Is it actually better?

18. A new model improves MAE by 8% but doubles inference latency.
    How should we evaluate that tradeoff?

19. We add BART after building entirely around buses.
    What assumptions might break?

20. The project suddenly gets 10,000 users from an X post.
    What are the FIRST things I'd worry about?


============================================================
U. CODE REVIEW MODE
============================================================

Whenever the agent generates a meaningful piece of code, I can ask:

"Walk me through this code as an engineer rather than just explaining the syntax."

The explanation should cover:

1. What responsibility does this code have?

2. Where does it sit in the architecture?

3. What calls it?

4. What does it call?

5. What data enters?

6. What data leaves?

7. What state does it read/write?

8. What can fail?

9. What assumptions does it make?

10. Which lines are framework boilerplate versus actual application logic?

11. Which part is most technically important?

12. What alternative implementation could we have used?

13. Why did we choose this one?

14. What would have to change at 100x scale?

15. What 2-3 things should I understand from this code rather than memorize?


============================================================
V. DESIGN REVIEW MODE
============================================================

Whenever we make an architectural decision, I can ask:

"Give me the engineering design review for this decision."

Answer:

PROBLEM
What problem are we solving?

CURRENT REQUIREMENTS
What does the system actually need today?

CHOICE
What are we doing?

ALTERNATIVES
What are 2-3 reasonable alternatives?

TRADEOFFS
What do we gain and lose with each?

DECISION
Why is our choice appropriate right now?

FAILURE CONDITION
What would make this choice stop working well?

MIGRATION
What would we probably move to next?

MEASUREMENT
What metric/evidence would tell us it's time to change?


============================================================
W. END-OF-SESSION CHECK
============================================================

At the end of a substantial coding session, ask me:

1. Draw/describe what changed in the architecture today.

2. What new data transformation did we introduce?

3. What was the most important design decision?

4. What alternative could we have chosen?

5. Why did we choose this approach?

6. What new failure mode did today's work introduce?

7. What part do I still not understand?

8. What should I read about based specifically on today's work?

9. Give me one debugging scenario based on what we built today.

10. Give me one technical-interview question based on what we built today.

Do not let me answer using vague phrases such as:
- "it's more scalable"
- "it's more efficient"
- "it's industry standard"
- "it's cleaner"
- "it's better for production"

Push me to explain WHAT resource, WHAT tradeoff, WHAT failure mode, WHAT measurement, and WHY.


============================================================
FINAL STANDARD
============================================================

By the end of this project I should be able to answer:

"Walk me through the system."

"Why did you design it this way?"

"What was the hardest technical problem?"

"What broke?"

"How did you debug it?"

"What happens when the system scales?"

"What would you redesign?"

"How does the data flow through the system?"

"How do you know the system is working correctly?"

"How would you improve reliability?"

"How would you improve performance?"

"How did you evaluate your ETA model?"

"How did you serve the model?"

"What did adding a second transit agency teach you?"

without relying on the coding agent to explain my own project back to me.