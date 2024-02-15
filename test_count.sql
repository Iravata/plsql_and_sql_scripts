DECLARE
    -- Flag to indicate if any scenario fails
    failure_found BOOLEAN := FALSE;

    -- Count variables
    count_table2 NUMBER;
    count_table3 NUMBER;

    -- Cursor to loop through scenario IDs from Table1
    CURSOR scenario_cursor IS
        SELECT scenario_id FROM Table1;

BEGIN
    -- Loop through each scenario ID
    FOR scenario_record IN scenario_cursor LOOP
        -- Adding a separator for each scenario for better readability
        DBMS_OUTPUT.PUT_LINE('----------------------------------------');
        DBMS_OUTPUT.PUT_LINE('Checking Scenario ID: ' || scenario_record.scenario_id);

        -- Query to count rows in Table2 for the current scenario
        SELECT COUNT(*) INTO count_table2 FROM Table2 WHERE scenario_id = scenario_record.scenario_id;

        -- Query to count rows in Table3 for the current scenario
        SELECT COUNT(*) INTO count_table3 FROM Table3 WHERE scenario_id = scenario_record.scenario_id;

        -- Output counts for each scenario
        DBMS_OUTPUT.PUT_LINE('Count in Table2: ' || count_table2 || ', Count in Table3: ' || count_table3);

        -- Check for failure condition
        IF (count_table2 > 0 AND count_table3 = 0) THEN
            -- Mark as failure found and output detailed information
            failure_found := TRUE;
            DBMS_OUTPUT.PUT_LINE('>>> FAILURE DETECTED <<<');
        ELSE
            -- Indicate a successful check for this scenario
            DBMS_OUTPUT.PUT_LINE('Status: Pass');
        END IF;

        -- Additional separator for clarity
        DBMS_OUTPUT.PUT_LINE('----------------------------------------');
    END LOOP;

    -- Summary of the checks
    DBMS_OUTPUT.PUT_LINE('=== SUMMARY OF CHECKS ===');
    IF failure_found THEN
        DBMS_OUTPUT.PUT_LINE('One or more scenarios failed. Please review the log for details.');
    ELSE
        DBMS_OUTPUT.PUT_LINE('All scenarios passed successfully.');
    END IF;
END;
