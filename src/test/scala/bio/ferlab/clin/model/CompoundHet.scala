package bio.ferlab.clin.model

case class CompoundHetInput(patient_id: String, chromosome: String, start: Long, reference: String, alternate: String, symbols: Seq[String], parental_origin: String)

case class HCComponent(symbol: String, locus: Seq[String])

case class CompoundHetOutput(patient_id: String, chromosome: String, start: Long, reference: String, alternate: String, is_hc: Boolean, hc_complement: Seq[HCComponent])

case class PossiblyCompoundHetInput(patient_id: String, chromosome: String, start: Long, reference: String, alternate: String, symbols: Seq[String])

case class PossiblyHCComponent(symbol: String, count: Long)

case class PossiblyCompoundHetOutput(patient_id: String, chromosome: String, start: Long, reference: String, alternate: String, is_possibly_hc: Boolean, possibly_hc_component: Seq[PossiblyHCComponent])