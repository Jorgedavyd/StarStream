# 0.0.1
First release

# 0.0.2
Added: GOES
Fixed: Limitless delivery.

# 1.0.3
Added:
- Optimization: Preprocessing with Polars.
    - Optimization: New date sampling methods for optimized behavior.
    - Methods:
    get_numpy
    get_torch
    get_pandas
    get_polars
    - Modularized the whole library for Satellite object.

# 1.1.0
Added:
- Optimization: Serialized heavy networks consumers to relieve the bottleneck.

# 1.1.1
- Solved:
    - Missing dates in between queries.
    - DSCOVR naming convention.
    - SMS preprocessing (raw)
    - testing scheme for SMS
