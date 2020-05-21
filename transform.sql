# Create Episode of Care Fact Table.
create table star.denormalized_eoc as
SELECT
  admission.subject_id,
  admission.hadm_id,
  time(admission.admittime) AS admittime,
  DATE(admission.admittime) AS admitdate,
  time(admission.dischtime) AS dischtime,
  DATE(admission.dischtime) AS dischdate,
  DATE(dob) AS dob,
  time(deathtime) AS deathtime,
  DATE(patients.dod) AS deathdate,
  admission_type,
  admission_location,
  discharge_location,
  insurance,
  LANGUAGE,
  gender,
  religion,
  marital_status,
  ethnicity,
  edregtime,
  edouttime,
  admission.diagnosis AS first_diagnosis,
  hospital_expire_flag,
  expire_flag,
  has_chartevents_data,
  a.procedure AS procedures,
  b.diagnosis AS diagnoses,
  outputevents.event AS outputevents,
  noteevents.noteevents,
  mbevents.microbiologyevents,
  le.labevents,
  co.callouts,
  cp.cptevents,
  tf.transfers,
  sv.services,
  drg.drgcodes,
  dte.datetimeevents,
  charte.chartevents,
  pmv.procedureevents_mv,
  icv.inputevents_cv,
  imv.inputevents_mv,
  psx.prescriptions,
FROM (
  SELECT
    * EXCEPT(row_id)
  FROM
    icu.admissions ) admission
LEFT JOIN (
  SELECT
    hadm_id,
    ARRAY_AGG(STRUCT(lst.icd9_code AS icd9,
        dict.short_title AS short_title,
        dict.long_title AS long_title)) AS procedure
  FROM
    icu.procedures_icd lst
  LEFT JOIN
    icu.d_icd_procedures dict
  ON
    lst.icd9_code = dict.icd9_code
  GROUP BY
    hadm_id ) a
ON
  admission.hadm_id = a.hadm_id
LEFT JOIN (
  SELECT
    hadm_id,
    ARRAY_AGG(STRUCT(lst.icd9_code AS icd9,
        dict.short_title AS short_title,
        dict.long_title AS long_title)) AS diagnosis
  FROM
    icu.diagnoses_icd lst
  LEFT JOIN
    icu.d_icd_diagnoses dict
  ON
    lst.icd9_code = dict.icd9_code
  GROUP BY
    hadm_id ) b
ON
  admission.hadm_id = b.hadm_id
LEFT JOIN (
  SELECT
    * EXCEPT(row_id)
  FROM
    icu.patients ) patients
ON
  admission.subject_id = patients.subject_id
LEFT JOIN (
  SELECT
    hadm_id,
    ARRAY_AGG(STRUCT(icustay_id,
        charttime,
        itemid,
        value,
        valueuom,
        storetime,
        STRUCT(outputevents.cgid,
          cg.label AS label,
          cg.description AS description) AS caregiver,
        stopped,
        newbottle,
        iserror)) event
  FROM
    icu.outputevents
  LEFT JOIN
    icu.caregivers cg
  ON
    cg.cgid = outputevents.cgid
  GROUP BY
    hadm_id ) outputevents
ON
  outputevents.hadm_id = admission.hadm_id
LEFT JOIN (
  SELECT
    hadm_id,
    ARRAY_AGG(STRUCT(chartdate,
        charttime,
        storetime,
        category,
        noteevents.description,
        STRUCT(noteevents.cgid,
          cg.label AS label,
          cg.description AS description) AS caregiver,
        iserror,
        text)) noteevents
  FROM
    icu.noteevents
  LEFT JOIN
    icu.caregivers cg
  ON
    noteevents.cgid = cg.cgid
  GROUP BY
    hadm_id ) noteevents
ON
  noteevents.hadm_id = admission.hadm_id
LEFT JOIN (
  SELECT
    hadm_id,
    ARRAY_AGG(STRUCT(chartdate,
        charttime,
        spec_itemid,
        spec_type_desc,
        org_itemid,
        org_name,
        isolate_num,
        ab_itemid,
        ab_name,
        dilution_text,
        dilution_comparison,
        dilution_value,
        interpretation)) AS microbiologyevents
  FROM
    icu.microbiologyevents
  GROUP BY
    hadm_id) mbevents
ON
  mbevents.hadm_id = admission.hadm_id
LEFT JOIN (
  SELECT
    hadm_id,
    ARRAY_AGG(STRUCT(itemid,
        charttime,
        value,
        valuenum,
        valueuom,
        flag)) labevents
  FROM
    icu.labevents
  GROUP BY
    hadm_id) le
ON
  le.hadm_id = admission.hadm_id
LEFT JOIN (
  SELECT
    hadm_id,
    ARRAY_AGG(STRUCT(submit_wardid,
        submit_careunit,
        curr_wardid,
        curr_careunit,
        callout_wardid,
        callout_service,
        request_tele,
        request_resp,
        request_cdiff,
        request_mrsa,
        request_vre,
        callout_status,
        callout_outcome,
        discharge_wardid,
        acknowledge_status,
        createtime,
        updatetime,
        acknowledgetime,
        outcometime,
        firstreservationtime,
        currentreservationtime)) callouts
  FROM
    icu.callout
  GROUP BY
    hadm_id) co
ON
  co.hadm_id = admission.hadm_id
LEFT JOIN (
  SELECT
    hadm_id,
    ARRAY_AGG(STRUCT(costcenter,
        chartdate,
        cpt_cd,
        cpt_number,
        cpt_suffix,
        ticket_id_seq,
        sectionheader,
        subsectionheader,
        description)) cptevents
  FROM
    icu.cptevents
  GROUP BY
    hadm_id) cp
ON
  cp.hadm_id = admission.hadm_id
LEFT JOIN (
  SELECT
    hadm_id,
    ARRAY_AGG(STRUCT(icustay_id,
        dbsource,
        eventtype,
        prev_careunit,
        curr_careunit,
        prev_wardid,
        curr_wardid,
        intime,
        outtime,
        los)) transfers
  FROM
    icu.transfers
  GROUP BY
    hadm_id) tf
ON
  tf.hadm_id = admission.hadm_id
LEFT JOIN (
  SELECT
    hadm_id,
    ARRAY_AGG(STRUCT(transfertime,
        prev_service,
        curr_service)) services
  FROM
    icu.services
  GROUP BY
    hadm_id) sv
ON
  sv.hadm_id = admission.hadm_id
LEFT JOIN (
  SELECT
    hadm_id,
    ARRAY_AGG(STRUCT(drg_type,
        drg_code,
        description,
        drg_severity,
        drg_mortality)) drgcodes
  FROM
    icu.drgcodes
  GROUP BY
    hadm_id) drg
ON
  drg.hadm_id = admission.hadm_id
LEFT JOIN (
  SELECT
    hadm_id,
    ARRAY_AGG(STRUCT(icustay_id,
        itemid,
        charttime,
        storetime,
        STRUCT(datetimeevents.cgid,
          cg.label AS label,
          cg.description AS description) AS caregiver,
        value,
        valueuom,
        warning,
        error,
        resultstatus,
        stopped)) datetimeevents
  FROM
    icu.datetimeevents
  LEFT JOIN
    icu.caregivers cg
  ON
    cg.cgid = datetimeevents.cgid
  GROUP BY
    hadm_id) dte
ON
  dte.hadm_id = admission.hadm_id
LEFT JOIN (
  SELECT
    hadm_id,
    ARRAY_AGG(STRUCT(icustay_id,
        itemid,
        charttime,
        storetime,
        STRUCT(chartevents.cgid,
          cg.label AS label,
          cg.description AS description) AS caregiver,
        value,
        valuenum,
        valueuom,
        warning,
        error,
        resultstatus,
        stopped)) chartevents
  FROM
    icu.chartevents
  LEFT JOIN
    icu.caregivers cg
  ON
    cg.cgid = chartevents.cgid
  GROUP BY
    hadm_id) charte
ON
  charte.hadm_id = admission.hadm_id
LEFT JOIN (
  SELECT
    hadm_id,
    ARRAY_AGG( STRUCT(ICUSTAY_ID,
        STARTTIME,
        ENDTIME,
        ITEMID,
        VALUE,
        VALUEUOM,
        LOCATION,
        LOCATIONCATEGORY,
        STORETIME,
        STRUCT(procedureevents_mv.cgid,
          cg.label AS label,
          cg.description AS description) AS caregiver,
        ORDERID,
        LINKORDERID,
        ORDERCATEGORYNAME,
        SECONDARYORDERCATEGORYNAME,
        ORDERCATEGORYDESCRIPTION,
        ISOPENBAG,
        CONTINUEINNEXTDEPT,
        CANCELREASON,
        STATUSDESCRIPTION,
        COMMENTS_EDITEDBY,
        COMMENTS_CANCELEDBY,
        COMMENTS_DATE)) procedureevents_mv
  FROM
    icu.procedureevents_mv
  LEFT JOIN
    icu.caregivers cg
  ON
    cg.cgid = procedureevents_mv.cgid
  GROUP BY
    hadm_id) pmv
ON
  pmv.hadm_id = admission.hadm_id
LEFT JOIN (
  SELECT
    hadm_id,
    ARRAY_AGG(STRUCT(icustay_id,
        charttime,
        itemid,
        amount,
        amountuom,
        rate,
        rateuom,
        storetime,
        STRUCT(inputevents_cv.cgid,
          cg.label AS label,
          cg.description AS description) AS caregiver,
        orderid,
        linkorderid,
        stopped,
        newbottle,
        originalamount,
        originalamountuom,
        originalroute,
        originalrate,
        originalrateuom,
        originalsite)) inputevents_cv
  FROM
    icu.inputevents_cv
  LEFT JOIN
    icu.caregivers cg
  ON
    cg.cgid = inputevents_cv.cgid
  GROUP BY
    hadm_id) icv
ON
  icv.hadm_id = admission.hadm_id
LEFT JOIN (
  SELECT
    hadm_id,
    ARRAY_AGG(STRUCT(icustay_id,
        STARTTIME,
        ENDTIME,
        ITEMID,
        AMOUNT,
        AMOUNTUOM,
        RATE,
        RATEUOM,
        STORETIME,
        STRUCT(inputevents_mv.cgid,
          cg.label AS label,
          cg.description AS description) AS caregiver,
        ORDERID,
        LINKORDERID,
        ORDERCATEGORYNAME,
        SECONDARYORDERCATEGORYNAME,
        ORDERCOMPONENTTYPEDESCRIPTION,
        ORDERCATEGORYDESCRIPTION,
        PATIENTWEIGHT,
        TOTALAMOUNT,
        TOTALAMOUNTUOM,
        ISOPENBAG,
        CONTINUEINNEXTDEPT,
        CANCELREASON,
        STATUSDESCRIPTION,
        COMMENTS_EDITEDBY,
        COMMENTS_CANCELEDBY,
        COMMENTS_DATE,
        ORIGINALAMOUNT,
        ORIGINALRATE)) inputevents_mv
  FROM
    icu.inputevents_mv
  LEFT JOIN
    icu.caregivers cg
  ON
    cg.cgid = inputevents_mv.cgid
  GROUP BY
    hadm_id) imv
ON
  imv.hadm_id = admission.hadm_id
LEFT JOIN (
  SELECT
    hadm_id,
    ARRAY_AGG(STRUCT(icustay_id, startdate, enddate, drug_type, drug, drug_name_poe, drug_name_generic, formulary_drug_cd, gsn, ndc, prod_strength, dose_val_rx, dose_unit_rx, form_val_disp, form_unit_disp, route)) prescriptions
  FROM
    icu.prescriptions
  GROUP BY
    hadm_id) psx
ON
  psx.hadm_id = admission.hadm_id;

# Create the labevent table. 
create table star.fact_labevent as
select subject_id, hadm_id, itemid, charttime, value, valuenum, valueuom, flag labevent from icu.labevents;


# Create Lab Item Dimension
create table star.dim_labitem as
select itemid, label, fluid, category, loinc_code from icu.d_labitems;


# Create Items Dimension
create table star.dim_item as
select itemid, label, abbreviation, dbsource, linksto, category, unitname, param_type, conceptid from icu.d_items;


# Generate Date Dimension into dim_date
create table star.dim_date as
SELECT 
  d as id, 
  EXTRACT(
    YEAR 
    FROM 
      d
  ) AS year, 
  EXTRACT(
    MONTH 
    FROM 
      d
  ) AS month, 
  EXTRACT(
    DAY 
    FROM 
      d
  ) as day, 
  FORMAT_DATE('%w', d) AS day_of_week, 
  FORMAT_DATE('%j', d) as day_of_year, 
  FORMAT_DATE('%Q', d) as quarter, 
  EXTRACT(
    WEEK 
    FROM 
      d
  ) AS week, 
  CASE WHEN FORMAT_DATE('%A', d) IN ('Sunday', 'Saturday') THEN True ELSE False END AS weekend, 
  FORMAT_DATE('%A', d) AS day_name, 
  FORMAT_DATE('%B', d) as month_name, 
FROM 
  (
    SELECT 
      * 
    FROM 
      UNNEST(
        GENERATE_DATE_ARRAY(
          '2000-01-01', '2999-01-01', INTERVAL 1 DAY
        )
      ) AS d
  );


# Generate time dimension into dim_time
create table star.dim_time as 
SELECT 
  TIME(d) AS id, 
  FORMAT_TIMESTAMP('%H', d) AS hour, 
  FORMAT_TIMESTAMP('%M', d) AS minute, 
  FORMAT_TIMESTAMP('%S', d) AS second, 
  MOD(
    CAST(
      FORMAT_TIMESTAMP('%H', d) as int64
    ), 
    12
  ) as hour12, 
  CASE WHEN CAST(
    FORMAT_TIMESTAMP('%H', d) as int64
  ) >= 12 THEN 'PM' ELSE 'AM' END as AMPM, 
FROM 
  (
    SELECT 
      * 
    FROM 
      UNNEST(
        GENERATE_TIMESTAMP_ARRAY(
          '2020-03-01 00:00:00', '2020-03-01 23:59:59', 
          INTERVAL 1 SECOND
        )
      ) AS d
  );