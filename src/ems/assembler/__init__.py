"""The assembler: turns a household's four input streams into ordered steps (ADR 0017, spec 011).

Pure, like the domain core: no runtime, I/O, clock or randomness. The actors and the offline side
run the same functions, so a household's steps can be compared bit for bit:
- ``mapping``: a wire event into a reading, a price curve or a forecast run (the shared mapping);
- ``meter_stream``: one meter stream's slots, duplicates, lateness and estimates;
- ``sparse_streams``: visibility and watermarks of the price and forecast streams;
- ``join``: releases the household's steps in order, once every needed watermark covers them;
- ``offline``: the offline side, folding recorded streams and stepping each strategy;
- ``params``: the parameters of a run, sent to each actor by ``configure``.
"""
