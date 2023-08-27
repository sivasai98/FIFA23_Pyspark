NAN = "nan"
INNER = "inner"
CNT = "cnt"
empty_string = ""

CSV = "csv"
HEADER = "header"
DELIMITER = "delimiter"
CSV_DELIMITER = ","
INFERSCHEMA = "inferSchema"
TRUE = "true"

REM_UN_CHAR1 = "[^A-Za-z0-9.]"
REM_UN_CHAR2 = ".*>|\""
REM_UN_CHAR3 = '\"|<[^>]+>|\"'
REM_UN_CHAR4 ="[^A-Za-z0-9. ]"

FIFA23_OFFICIAL_DATA = "data/FIFA23_official_data.csv"
FIFA_CLUBS_DATA = "data/FIFA_Clubs.csv"

case_st = """case when Position in ('LS','RS','RW','LW','LF','RF','CF','ST') then 'ATTACKERS'
                  when Position in ('LCM','RCM','RDM','LM','CDM','RM','CAM','LDM','CM','RAM','LAM') then 'DEFENDER'
                  when Position in ('LB','LCB','RB','RWB','LWB','RCB','CB') then 'MIDFIELDER'
                  when Position in ('GK') then 'GOALKEEPER'
                  when Position in ('SUB') then 'SUBSTITUTE'
                  when Position in ('RES') then 'RESERVE'
                  else 'Unknown' end
               """
