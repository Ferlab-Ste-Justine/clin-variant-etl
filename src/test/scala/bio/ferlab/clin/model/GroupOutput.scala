/**
 * Generated by [[bio.ferlab.datalake.spark3.ClassGenerator]]
 * on 2021-03-10T16:12:15.038
 */
package bio.ferlab.clin.model

import java.sql.Timestamp


case class GroupOutput(//`group_id`: String = "13636",
                       `id`: String = "13636",
                       `actual`: Boolean = true,
                       `family_structure_code`: String = "DUO",
                       `resource_type`: String = "Group",
                       `type`: String = "person",
                       `ingestion_file_name`: String = "raw/landing/fhir/Group/Group_0_19000101_130549.json",
                       `ingested_on`: Timestamp = Timestamp.valueOf("2021-03-10 13:05:49.0"),
                       `version_id`: String = "1",
                       `updated_on`: Timestamp = Timestamp.valueOf("2021-02-05 11:21:27.111"),
                       `created_on`: Timestamp = Timestamp.valueOf("2021-02-05 11:21:27.111"),
                       `members`: List[String] = List("13634", "13635"))

