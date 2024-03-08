BEGIN
  FOR r IN (SELECT column_name FROM table_name) LOOP
    BEGIN
      -- Attempt to convert the string to a number
      DECLARE
        v_number NUMBER := TO_NUMBER(r.column_name);
    EXCEPTION
      WHEN VALUE_ERROR THEN
        -- Output values that cannot be converted to a number
        DBMS_OUTPUT.PUT_LINE('Non-numeric value found: ' || r.column_name);
    END;
  END LOOP;
END;
