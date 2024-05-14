// reading nested fileds from s3 (from Dynampodb source)


CREATE TABLE edr.med_cab_prescriptions

      AS

       SELECT

           JSON_EXTRACT_PATH_TEXT(A.str_item, 'id', 'S') AS id,

       JSON_EXTRACT_PATH_TEXT(A.str_item, '__typename', 'S'

      ) AS type_name,

           CASE

       WHEN JSON_EXTRACT_PATH_TEXT(A.str_item, 'account', 'S') <> ''

           THEN JSON_EXTRACT_PATH_TEXT(A.str_item, 'account', 'S')

       ELSE NULL

       END AS account,

       JSON_EXTRACT_PATH_TEXT(A.str_item, 'activityat', 'S') AS activityat,

       JSON_EXTRACT_PATH_TEXT(A.str_item, 'archived', 'S') AS  archived,

       JSON_EXTRACT_PATH_TEXT(A.str_item, 'authorizedrefills', 'S') AS authorizedrefills,

       CASE

      WHEN JSON_EXTRACT_PATH_TEXT(A.str_item, 'completedat', 'S') <> ''

          THEN TIMESTAMP 'epoch' + CAST(JSON_EXTRACT_PATH_TEXT(A.str_item, 'completedat', 'S') AS BIGINT) / 1000 * INTERVAL '1 second'

      ELSE NULL

      END AS completed_at,

       JSON_EXTRACT_PATH_TEXT(A.str_item, 'date', 'S') AS DATE,

       JSON_EXTRACT_PATH_TEXT(A.str_item, 'daysSupply', 'N') AS daysSupply,

       JSON_EXTRACT_PATH_TEXT(A.str_item, 'deleted', 'BOOL') AS deleted,

--drug

   CASE

       WHEN JSON_EXTRACT_PATH_TEXT(A.str_item, 'drug', 'M')  <> ''

       THEN JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_PATH_TEXT(A.str_item, 'drug', 'M'), 'name', 'S')

       ELSE NULL

       END AS drug_name,

   CASE

       WHEN JSON_EXTRACT_PATH_TEXT(A.str_item, 'drug', 'M')  <> ''

       THEN JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_PATH_TEXT(A.str_item, 'drug', 'M'), 'dosage', 'S')

       ELSE NULL

       END AS drug_dosage,

   CASE

       WHEN JSON_EXTRACT_PATH_TEXT(A.str_item, 'drug', 'M')  <> ''

           THEN JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_PATH_TEXT(A.str_item, 'drug', 'M'), 'id', 'S')

       ELSE NULL

       END AS drug_id,

   CASE

       WHEN JSON_EXTRACT_PATH_TEXT(A.str_item, 'drug', 'M')  <> ''

           THEN JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_PATH_TEXT(A.str_item, 'drug', 'M'), 'ndc', 'S')

       ELSE NULL

       END AS drug_ndc,

   CASE

       WHEN JSON_EXTRACT_PATH_TEXT(A.str_item, 'drug', 'M')  <> ''

           THEN JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_PATH_TEXT(A.str_item, 'drug', 'M'), 'form', 'S')

       ELSE NULL

       END AS drug_form,

   CASE

       WHEN JSON_EXTRACT_PATH_TEXT(A.str_item, 'expiration', 'N')  <> ''

           THEN TIMESTAMP 'epoch' + CAST(JSON_EXTRACT_PATH_TEXT(A.str_item, 'expiration', 'N') AS BIGINT) / 1000 * INTERVAL '1 second'

       ELSE NULL

       END AS expiration,

   CASE

       WHEN JSON_EXTRACT_PATH_TEXT(A.str_item, 'lastUpdated', 'S') <> '' THEN JSON_EXTRACT_PATH_TEXT(A.str_item, 'lastUpdated', 'S')::TIMESTAMP

       ELSE NULL

       END AS lastUpdated,

 CASE

       WHEN JSON_EXTRACT_PATH_TEXT(A.str_item, 'owner', 'S') <> ''

           THEN JSON_EXTRACT_PATH_TEXT(A.str_item, 'owner', 'S')

       ELSE NULL

       END AS OWNER,

--pharmacy

CASE

       WHEN JSON_EXTRACT_PATH_TEXT(A.str_item, 'pharmacy', 'M') <> ''

       THEN JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_PATH_TEXT(A.str_item, 'pharmacy', 'M'), 'id', 'S')

       ELSE NULL

       END AS pharmacy_id,

   CASE

       WHEN JSON_EXTRACT_PATH_TEXT(A.str_item, 'pharmacy', 'M') <> ''

           THEN JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_PATH_TEXT(A.str_item, 'pharmacy', 'M'), 'name', 'S')

       ELSE NULL

       END AS pharmacy_name,

   CASE

       WHEN JSON_EXTRACT_PATH_TEXT(A.str_item, 'pharmacy', 'M') <> ''

           THEN JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_PATH_TEXT(A.str_item, 'pharmacy', 'M'), 'npi', 'S')

       ELSE NULL

       END AS pharmacy_npi,

   CASE

       WHEN JSON_EXTRACT_PATH_TEXT(A.str_item, 'pharmacy', 'M') <> ''

           THEN JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_PATH_TEXT(A.str_item, 'pharmacy', 'M'), 'parentId', 'N')

       ELSE NULL

       END AS pharmacy_parentId,

       CASE

       WHEN JSON_EXTRACT_PATH_TEXT(A.str_item, 'pharmacy', 'M') <> '' THEN JSON_EXTRACT_PATH_TEXT(

               JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_PATH_TEXT(A.str_item, 'pharmacy', 'M'), 'physicalAddress', 'M'), 'line1', 'S')

       ELSE NULL

       END AS pharmacy_physicalAddress_line1,

   CASE

       WHEN JSON_EXTRACT_PATH_TEXT(A.str_item, 'pharmacy', 'M') <> '' THEN JSON_EXTRACT_PATH_TEXT(

               JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_PATH_TEXT(A.str_item, 'pharmacy', 'M'), 'physicalAddress', 'M'), 'zip', 'S')

       ELSE NULL

       END AS pharmacy_physicalAddress_zip,

   CASE

       WHEN JSON_EXTRACT_PATH_TEXT(A.str_item, 'pharmacy', 'M') <> '' THEN JSON_EXTRACT_PATH_TEXT(

               JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_PATH_TEXT(A.str_item, 'pharmacy', 'M'), 'physicalAddress', 'M'), 'state', 'S')

       ELSE NULL

       END AS pharmacy_physicalAddress_state,

   CASE

       WHEN JSON_EXTRACT_PATH_TEXT(A.str_item, 'pharmacy', 'M') <> '' THEN JSON_EXTRACT_PATH_TEXT(

               JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_PATH_TEXT(A.str_item, 'pharmacy', 'M'), 'physicalAddress', 'M'), 'city', 'S')

       ELSE NULL

       END AS pharmacy_physicalAddress_city,

   CASE

       WHEN JSON_EXTRACT_PATH_TEXT(A.str_item, 'pharmacy', 'M') <> ''

           THEN JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_PATH_TEXT(A.str_item, 'pharmacy', 'M'), 'phone', 'S')

       ELSE NULL

       END AS pharmacy_phone,

   CASE

       WHEN JSON_EXTRACT_PATH_TEXT(A.str_item, 'pharmacy', 'M') <> ''

           THEN JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_PATH_TEXT(A.str_item, 'pharmacy', 'M'), 'fax', 'S')

       ELSE NULL

       END AS pharmacy_fax,

--prescriber

       CASE

       WHEN JSON_EXTRACT_PATH_TEXT(A.str_item, 'prescriber', 'M') <> ''

           THEN JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_PATH_TEXT(A.str_item, 'prescriber', 'M'), 'npi', 'S')

       ELSE NULL

       END AS prescriber_npi,

       CASE WHEN JSON_EXTRACT_PATH_TEXT(A.str_item, 'prescriber', 'M') <> ''

           THEN JSON_EXTRACT_PATH_TEXT(

               JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_PATH_TEXT(A.str_item, 'prescriber', 'M'), 'physicalAddress', 'M'),  'line1', 'S')

       ELSE NULL

       END AS prescriber_physicalAddress_line1,

   CASE WHEN JSON_EXTRACT_PATH_TEXT(A.str_item, 'prescriber', 'M') <> ''

           THEN JSON_EXTRACT_PATH_TEXT(

               JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_PATH_TEXT(A.str_item, 'prescriber', 'M'), 'physicalAddress', 'M'),  'line2', 'S')

       ELSE NULL

       END AS prescriber_physicalAddress_line2,

   CASE

       WHEN JSON_EXTRACT_PATH_TEXT(A.str_item, 'prescriber', 'M') <> '' THEN JSON_EXTRACT_PATH_TEXT(

               JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_PATH_TEXT(A.str_item, 'prescriber', 'M'), 'physicalAddress', 'M'), 'zip', 'S')

       ELSE NULL

       END AS prescriber_physicalAddress_zip,

   CASE

       WHEN JSON_EXTRACT_PATH_TEXT(A.str_item, 'prescriber', 'M') <> '' THEN JSON_EXTRACT_PATH_TEXT(

               JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_PATH_TEXT(A.str_item, 'prescriber', 'M'), 'physicalAddress', 'M'), 'state', 'S')

       ELSE NULL

       END AS prescriber_physicalAddress_state,

   CASE

       WHEN JSON_EXTRACT_PATH_TEXT(A.str_item, 'prescriber', 'M') <> '' THEN JSON_EXTRACT_PATH_TEXT(

               JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_PATH_TEXT(A.str_item, 'prescriber', 'M'), 'physicalAddress', 'M'), 'city', 'S')

       ELSE NULL

       END AS prescriber_physicalAddress_city,

   CASE WHEN JSON_EXTRACT_PATH_TEXT(A.str_item, 'prescriber', 'M') <> ''

           THEN JSON_EXTRACT_PATH_TEXT(

               JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_PATH_TEXT(A.str_item, 'prescriber', 'M'), 'mailingAddress', 'M'),  'line1', 'S')

       ELSE NULL

       END AS prescriber_mailingAddress_line1,

   CASE WHEN JSON_EXTRACT_PATH_TEXT(A.str_item, 'prescriber', 'M') <> ''

           THEN JSON_EXTRACT_PATH_TEXT(

               JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_PATH_TEXT(A.str_item, 'prescriber', 'M'), 'mailingAddress', 'M'),  'line2', 'S')

       ELSE NULL

       END AS prescriber_mailingAddress_line2,

   CASE

       WHEN JSON_EXTRACT_PATH_TEXT(A.str_item, 'prescriber', 'M') <> '' THEN JSON_EXTRACT_PATH_TEXT(

               JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_PATH_TEXT(A.str_item, 'prescriber', 'M'), 'mailingAddress', 'M'), 'zip', 'S')

       ELSE NULL

       END AS prescriber_mailingAddress_zip,

   CASE

       WHEN JSON_EXTRACT_PATH_TEXT(A.str_item, 'prescriber', 'M') <> '' THEN JSON_EXTRACT_PATH_TEXT(

               JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_PATH_TEXT(A.str_item, 'prescriber', 'M'), 'mailingAddress', 'M'), 'state', 'S')

       ELSE NULL

       END AS prescriber_mailingAddress_state,

   CASE

       WHEN JSON_EXTRACT_PATH_TEXT(A.str_item, 'prescriber', 'M') <> '' THEN JSON_EXTRACT_PATH_TEXT(

               JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_PATH_TEXT(A.str_item, 'prescriber', 'M'), 'mailingAddress', 'M'), 'city', 'S')

       ELSE NULL

       END AS prescriber_mailingAddress_city,

   JSON_EXTRACT_PATH_TEXT(A.str_item, 'quantity', 'N') AS quantity,

   JSON_EXTRACT_PATH_TEXT(A.str_item, 'refillsRemaining', 'N') AS refillsRemaining,

   JSON_EXTRACT_PATH_TEXT(A.str_item, 'rxNumber', 'S') AS rxNumber,

--savings

   CASE

       WHEN JSON_EXTRACT_PATH_TEXT(A.str_item, 'savings', 'M') <> ''

           THEN JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_PATH_TEXT(A.str_item, 'savings', 'M'), 'amount', 'N')

       ELSE NULL

       END AS savings_amount,

   JSON_EXTRACT_PATH_TEXT(A.str_item, 'source', 'S') AS source,

--source_context

               CASE

       WHEN JSON_EXTRACT_PATH_TEXT(A.str_item, 'sourceContext', 'M') <> ''

           THEN JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_PATH_TEXT(A.str_item, 'sourceContext', 'M'), 'location', 'S')

       ELSE NULL

       END AS sourceContext_location,

               CASE

       WHEN JSON_EXTRACT_PATH_TEXT(A.str_item, 'sourceContext', 'M') <> ''

           THEN JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_PATH_TEXT(A.str_item, 'sourceContext', 'M'), 'platform', 'S')

       ELSE NULL

       END AS sourceContext_platform,

       JSON_EXTRACT_PATH_TEXT(A.str_item, 'sourceId', 'S') AS sourceId,

   CASE

       WHEN JSON_EXTRACT_PATH_TEXT(A.str_item, 'startedAt', 'N')  <> ''

           THEN TIMESTAMP 'epoch' + CAST(JSON_EXTRACT_PATH_TEXT(A.str_item, 'startedAt', 'N') AS BIGINT) / 1000 * INTERVAL '1 second'

       ELSE NULL

       END AS started_at,

       JSON_EXTRACT_PATH_TEXT(A.str_item, 'state', 'S') AS state

FROM (SELECT

         json_serialize(pre.item) AS str_item

     FROM edr_pii_raw.med_cab_prescriptions pre) A
