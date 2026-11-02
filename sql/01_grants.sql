--- Analysts access -----

GRANT USE CATALOG ON CATALOG ab_power TO `ab_power_analysts`;
GRANT USE SCHEMA ON SCHEMA ab_power.gold TO `ab_power_analysts`;
GRANT SELECT ON SCHEMA ab_power.gold TO `ab_power_analysts`;


----Data Engineers Access -----
GRANT USE CATALOG ON CATALOG ab_power TO `ab_power_engineers`;
GRANT USE SCHEMA ON SCHEMA ab_power.silver TO `ab_power_engineers`;
GRANT USE SCHEMA ON SCHEMA ab_power.bronze TO `ab_power_engineers`;
GRANT USE SCHEMA ON SCHEMA ab_power.gold TO `ab_power_engineers`;
GRANT USE SCHEMA ON SCHEMA ab_power.landing TO `ab_power_engineers`;
GRANT CREATE TABLE, CREATE MATERIALIZED VIEW, SELECT, MODIFY ON SCHEMA ab_power.bronze TO `ab_power_engineers`;
GRANT CREATE TABLE, CREATE MATERIALIZED VIEW, SELECT, MODIFY ON SCHEMA ab_power.silver TO `ab_power_engineers`;
GRANT CREATE TABLE, CREATE MATERIALIZED VIEW, SELECT, MODIFY ON SCHEMA ab_power.gold TO `ab_power_engineers`;
GRANT READ VOLUME, WRITE VOLUME ON VOLUME ab_power.landing.raw_files TO `ab_power_engineers`;


SHOW GRANTS ON SCHEMA ab_power.gold;
