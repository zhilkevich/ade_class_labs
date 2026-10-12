USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE API INTEGRATION GIT_GITHUB_INT_ADE
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/zhilkevich')
  ENABLED = TRUE;


-- When creating a Workspace from Git repository use the following:
-- Repository URL: https://github.com/zhilkevich/ade_class_labs
-- Workspace Name: snowflake-fundamentals
-- API Integration: GIT_GITHUB_INT
-- Public repository
