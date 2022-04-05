CREATE OR REPLACE TEMPORARY TABLE WORKSPACE_DB.WORKSPACE.PEOPLE_PROFILES AS
SELECT UUID_STRING() PERSON_ID, audience_array
FROM (
SELECT UUID_STRING(), ABS(MOD(RANDOM(), 10000)) audience_group
FROM TABLE (generator(rowcount => 1000000))) P
inner join
(select audience_group,
arrayagg(distinct audience_id) within group (order by audience_id) audience_array
from (
select ABS(MOD(RANDOM(), 10000)) audience_group,
ABS(MOD(RANDOM(), 2000)) audience_id
from table (generator(rowcount => 1000000)) v) A
group by audience_group) AG ON AG.audience_group = P.AUDIENCE_GROUP
order by array_size(audience_array)
;
