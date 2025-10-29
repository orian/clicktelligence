
# ProfileEvents

1. suck https://github.com/ClickHouse/ClickHouse/blob/2f8e348386823f92892a818d66753d81f7641723/src/Common/ProfileEvents.cpp#L1465
2. for a given query_id fetch:
   * query_log
   * processor_*_log
   * trace_log
   * ???
3. should be able to select two queries from a history / version tree and compare it. 