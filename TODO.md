# Courseware TODO

- [ ] Update the pipeline progression PDF diagrams and their lab references.

  Show schema-qualified object names so Bronze, Silver, Gold and OPS are clear. Label object types (TABLE, STREAM, VIEW, PIPE, TASK, PROCEDURE) and distinguish actions such as COPY INTO, INSERT, MERGE, CALL and EXECUTE TASK by shape and label. Keep data-flow and control-flow arrows distinguishable. Coordinate diagram names with the proposed SQL/notebook renames: SILVER.ORDER_EVENTS_ENRICHED, SILVER.ORDER_EVENTS_CANDIDATES and SILVER.ORDERS; these renames have not yet been implemented. Replace lab references to Markdown diagrams with PDF page references; retain needed explanations before retiring the duplicate Markdown diagram files. Day 1 already has a PDF; prepare equivalent PDFs before retiring Day 2-3 Markdown diagrams. Update the course ZIP and Git copies after visual review. The repository Day 1 PDF previously remained locked; ensure its updated copy is synchronized.

- [ ] When updating the PDF, update slide 64 Scripts and speaker notes to reference the consolidated 06-Snowpark-Development.ipynb workflow; remove the archived SQL companion reference.
