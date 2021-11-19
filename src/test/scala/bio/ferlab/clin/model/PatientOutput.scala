/**
 * Generated by [[bio.ferlab.datalake.spark3.ClassGenerator]]
 * on 2021-11-19T10:36:13.664191
 */
package bio.ferlab.clin.model

import java.sql.{Date, Timestamp}


case class PatientOutput(`id`: String = "PA00004",
                         `ingested_on`: Timestamp = Timestamp.valueOf("2021-11-18 16:31:03.0"),
                         `medical_record_number`: String = "MRN-283776",
                         `active`: Boolean = true,
                         `birth_date`: Date = Date.valueOf("1995-08-05"),
                         `gender`: String = "male",
                         `resource_type`: String = "Patient",
                         `ingestion_file_name`: String = "/raw/landing/fhir/Patient/Patient_0_20211118_163103.json",
                         `version_id`: String = "3",
                         `updated_on`: Timestamp = Timestamp.valueOf("2021-11-17 17:29:55.843"),
                         `created_on`: Timestamp = Timestamp.valueOf("2021-11-17 17:29:55.843"),
                         `jurisdictional_health_number`: String = "BOUR08059572",
                         `practitioner_role_id`: String = "PRR00101",
                         `organization_id`: Option[String] = Some("CHUSJ"),
                         `family_id`: String = "FA004",
                         `is_fetus`: Boolean = false,
                         `is_proband`: Boolean = true,
                         `family_relationship`: List[FAMILY_RELATIONSHIP] = List(FAMILY_RELATIONSHIP(), FAMILY_RELATIONSHIP("PA00005", "MTH")))

case class FAMILY_RELATIONSHIP(`patient2`: String = "PA00006",
                               `patient1_to_patient2_relation`: String = "FTH")
