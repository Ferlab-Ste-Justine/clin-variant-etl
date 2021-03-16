/**
 * Generated by [[bio.ferlab.datalake.core.ClassGenerator]]
 * on 2021-03-10T15:41:07.029
 */
package bio.ferlab.clin.model

import java.sql.{Date, Timestamp}


case class ClinicalImpressionOutput(`date`: Date = Date.valueOf("2019-06-20"),
                                    `ageInDays`: Long = 1315,
                                    `id`: String = "CI0005",
                                    `investigation`: List[INVESTIGATION] = List(INVESTIGATION()),
                                    `resourceType`: String = "ClinicalImpression",
                                    `status`: String = "in-progress",
                                    `ingestionFileName`: String = "/raw/landing/fhir/ClinicalImpression/ClinicalImpression_0_19000101_102715.json",
                                    `ingestedOn`: Timestamp = Timestamp.valueOf("2021-03-10 10:27:15.0"),
                                    `versionId`: String = "25",
                                    `updatedOn`: Timestamp = Timestamp.valueOf("2020-12-17 13:14:41.572"),
                                    `createdOn`: Timestamp = Timestamp.valueOf("2020-12-17 13:14:41.572"),
                                    `patientId`: String = "PA0005",
                                    `practitionerId`: String = "PR00105")


case class ITEM(`reference`: String = "Observation/OB0157")

case class ClinicalImpressionCODE(`text`: String = "initial-examination")

case class INVESTIGATION(`code`: ClinicalImpressionCODE = ClinicalImpressionCODE(),
                         `item`: List[ITEM] = List(ITEM(), ITEM("Observation/OB0206"), ITEM("Observation/OB0255"), ITEM("Observation/OB0005"),
                           ITEM("Observation/OB0054"), ITEM("Observation/OB0070"), ITEM("Observation/OB0108"), ITEM("FamilyMemberHistory/FMH0005")))

