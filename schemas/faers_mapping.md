FAERS Mapping Notes
=======

# Field types

## patient.drug.drugadministrationroute

Should be a short but we want to keep the leading 0, so sticking with a string

## patient.drug.drugcumulativedosageunit

Should be a short but we want to keep the leading 0, so sticking with a string

## patient.drug.drugenddateformat

Should be a short but we want to keep the leading 0, so sticking with a string

## reaction.reactionmeddrapt

This is in ```ALL CAPS``` in AERS and ```Not all caps``` in FAERS. We use the ```upper_case_not_tokenized``` analyzer for ```reaction.reactionmeddrapt.exact``` as a workaround so that count requests work properly.

# Fields in FAERS vs AERS

## Fields only in FAERS

* duplicate
* occurcountry
* patient.drug.actiondrug
* patient.drug.drugadditional
* patient.drug.drugcumulativedosagenumb
* patient.drug.drugcumulativedosageunit
* patient.drug.drugdosageform
* patient.drug.drugintervaldosagedefinition
* patient.drug.drugintervaldosageunitnumb
* patient.drug.drugrecurreadministration
* patient.drug.drugseparatedosagenumb
* patient.drug.drugstructuredosagenumb
* patient.drug.drugstructuredosageunit
* reaction.reactionmeddraversionpt
* reaction.reactionoutcome
* primarysourcecountry (not to be confused with primarysource.reportercountry)
* receiver.receivertype
* receiver.receiverorganization
* reportduplicate.duplicatesource
* reportduplicate.duplicatenumb
* safetyreportversion
* sender.sendertype

## Fields only in AERS

* patient.patientdeath.patientdeathdateformat
* patient.patientdeath.patientdeathdate

## Fields in both AERS and FAERS

* companynumb
* patient.drug.drugadministrationroute
* patient.drug.drugauthorizationnumb
* patient.drug.drugbatchnumb
* patient.drug.drugcharacterization
* patient.drug.drugdoseagetext
* patient.drug.drugenddate
* patient.drug.drugenddateformat
* patient.drug.drugindication
* patient.drug.drugstartdate
* patient.drug.drugstartdateformat
* patient.drug.drugtreatmentduration
* patient.drug.drugtreatmentdurationunit
* patient.drug.medicinalproduct
* patient.patientonsetage
* patient.patientonsetageunit
* patient.patientsex
* patient.patientweight
* reaction.reactionmeddrapt
* primarysource.qualification
* receivedate
* receivedateformat
* receiptdate
* receiptdateformat
* safetyreport
* sender.senderorganization
* serious
* seriousnesscongenitalanomali
* seriousnessdeath
* seriousnessdisabling
* seriousnesshospitalization
* seriousnesslifethreatening
* seriousnessother
* transmissiondate
* transmissiondateformat

# Values in AERS and FAERS

## primarysource.reportercountry

### AERS

Full coutry names such as ```United States``` and ```Japan```.

### FAERS

Two letter country codes such as ```US``` and ```JP```.
