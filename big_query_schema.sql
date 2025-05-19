create table `hospital.health_record`(
    patient_id STRING NOT NULL,
    hospital_id STRING NOT NULL,
    diagnosis_code STRING NOT NULL,
    disease STRING,
    diagnosis_date DATE,
    treatment STRING,
    doctor STRING
);