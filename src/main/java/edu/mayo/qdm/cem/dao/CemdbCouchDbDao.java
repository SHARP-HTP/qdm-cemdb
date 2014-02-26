/*
 * Copyright: (c) 2004-2012 Mayo Foundation for Medical Education and 
 * Research (MFMER). All rights reserved. MAYO, MAYO CLINIC, and the
 * triple-shield Mayo logo are trademarks and service marks of MFMER.
 *
 * Except as contained in the copyright notice above, or as used to identify 
 * MFMER as the author of this software, the trade names, trademarks, service
 * marks, or product names of the copyright holder shall not be used in
 * advertising, promotion or otherwise in connection with this software without
 * prior written authorization of the copyright holder.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.mayo.qdm.cem.dao;

import edu.mayo.qdm.patient.*;
import org.apache.commons.collections.CollectionUtils;
import org.apache.log4j.Logger;

import java.text.ParseException;
import java.util.*;
import java.util.Map.Entry;

/**
 * The main CouchDB client to the CEM DB.
 *
 * @author <a href="mailto:kevin.peterson@mayo.edu">Kevin Peterson</a>
 */
@SuppressWarnings("unchecked")
public class CemdbCouchDbDao {
	
	protected final Logger log = Logger.getLogger(getClass());
	
	private CouchDbClient couchDbClient;

    private final static String DRUG_BY_PATIENT_ID_VIEW = 
    		"secondaryusenoteddrug/_design/drug_by_patient_id/_view/drug_by_patient_id";
    
    private final static String LAB_BY_PATIENT_ID_VIEW = 
    		"secondaryusestandardlab/_design/lab_by_patient_id/_view/lab_by_patient_id";
    
    private final static String DIAGNOSIS_BY_PATIENT_ID_VIEW = 
    		"administrativediagnosis/_design/diagnosis_by_patient_id/_view/diagnosis_by_patient_id";
    
    private final static String ALL_PATIENTS_VIEW = 
    		"secondaryusepatient2/_design/all_patients/_view/all_patients";
    
    private final CouchDbIterator.Transformer<Patient> patientTransformer = new PatientTransformer();
    
    private static final Map<String,String> GROUP_PARAM = new HashMap<String,String>();
    static {
    	GROUP_PARAM.put("group", "true");
    }
    
    /**
     * Instantiates a new cemdb couch db dao.
     */
    public CemdbCouchDbDao(){
    	super();
    	this.couchDbClient = new CouchDbClient();
    }
    
	/**
	 * Gets the patients.
	 *
	 * @return the patients
	 */
	public Iterable<Patient> getPatients() {
		
		CouchDbIterator<Patient> itr = new CouchDbIterator<Patient>(
				ALL_PATIENTS_VIEW, 
				null,
				GROUP_PARAM,
				this.patientTransformer, 
				new CouchDbIterator.PageDecorator<Patient>(){

					@Override
					public List<Patient> decorate(List<Patient> patients) {
						if(CollectionUtils.isEmpty(patients)){
							return patients;
						}
						
						Map<String,Patient> patientIds = new HashMap<String,Patient>();
						for (Patient patient : patients) {
							patientIds.put(patient.getSourcePid(), patient);
						}

						
						Map<String, List<Lab>> labEntrySet = getLabs(patientIds.keySet());
						for(Entry<String, List<Lab>> entry : labEntrySet.entrySet()){
							log.info("Labs count for patient " + entry.getKey() + " " + entry.getValue().size());
							
							Patient p = patientIds.get(entry.getKey());
							for(Lab lab : entry.getValue()){
								p.addLab(lab);
							}
						}
						
						Map<String, List<Medication>> medicationEntrySet = getDrugs(patientIds.keySet());
						for(Entry<String, List<Medication>> entry : medicationEntrySet.entrySet()){
							log.info("Drug count for patient " + entry.getKey() + " " + entry.getValue().size());
							
							Patient p = patientIds.get(entry.getKey());			
							for(Medication medication : entry.getValue()){
								p.addMedication(medication);
							}
						}
						
						Map<String, List<Diagnosis>> diagnosisEntrySet = getDiagnosises(patientIds.keySet());
						for(Entry<String, List<Diagnosis>> entry : diagnosisEntrySet.entrySet()){
							log.info("Problem count for patient " + entry.getKey() + " " + entry.getValue().size());
							
							Patient p = patientIds.get(entry.getKey());
							for(Diagnosis diagnosis : entry.getValue()){
								p.addDiagnosis(diagnosis);
							}
						}

						return patients;
					}
			
				},
				this.couchDbClient);
	
		return itr;
	}
	

	
	/**
	 * Gets the labs.
	 *
	 * @param patientIds the patient ids
	 * @return the labs
	 */
	public Map<String,List<Lab>> getLabs(Collection<String> patientIds) {
		Map<String, Object> results = this.queryView(LAB_BY_PATIENT_ID_VIEW, patientIds, null);

		Map<String,List<Lab>> labs = new HashMap<String,List<Lab>>();

		for (Map<String, Object> row : (List<Map<String,Object>>)MapUtils.get("rows", results)){
			String patientId = MapUtils.get("value.patientId", row).toString();
			String code = MapUtils.get("value.labResultCode", row).toString();

			String date = MapUtils.get("value.collectionDate", row).toString();
			String unit = MapUtils.get("value.unit", row).toString();
			if(!unit.equals("mg/dL")){
				unit = "mg/dL";
			}
			double value = Double.parseDouble(MapUtils.get("value.value", row).toString());

			Lab lab;
			try {
				lab = new Lab(
                        new Concept(code, CemDbUtils.LOINC, null),
                        new Value(Double.toString(value), unit),
                        CemDbUtils.CEMDB_DATE_FORMAT2.parse(date));
			} catch (ParseException e) {
				throw new IllegalStateException(e);
			}
			
			if(!labs.containsKey(patientId)){
				labs.put(patientId, new ArrayList<Lab>());
			}
			labs.get(patientId).add(lab);
		}
		
		return labs;
	}
	
	/**
	 * Gets the diagnosises.
	 *
	 * @param patientIds the patient ids
	 * @return the diagnosises
	 */
	public Map<String,List<Diagnosis>> getDiagnosises(Collection<String> patientIds) {
		Map<String, Object> results = this.queryView(DIAGNOSIS_BY_PATIENT_ID_VIEW, patientIds, null);
		
		Map<String,List<Diagnosis>> problems = new HashMap<String,List<Diagnosis>>();

		for (Map<String, Object> row : (List<Map<String,Object>>)MapUtils.get("rows", results)){
			String patientId = MapUtils.get("value.patientId", row).toString();
			String startingDate = "20030224111900";
			String endingDate = "20110224111900";
			String code = MapUtils.get("value.code", row).toString();

            Diagnosis problem = null;
			try {
				problem = new Diagnosis(
                        new Concept(code, CemDbUtils.SNOMEDCT, null),
						CemDbUtils.CEMDB_DATE_FORMAT2.parse(startingDate),
						CemDbUtils.CEMDB_DATE_FORMAT2.parse(endingDate));
			} catch (ParseException e) {
				log.warn(e);
                continue;
			}
			
			if(!problems.containsKey(patientId)){
				problems.put(patientId, new ArrayList<Diagnosis>());
			}
			problems.get(patientId).add(problem);
		}
		
		return problems;
	}

	/**
	 * Gets the drugs.
	 *
	 * @param patientIds the patient ids
	 * @return the drugs
	 */
	public Map<String,List<Medication>> getDrugs(Collection<String> patientIds) {
		Map<String, Object> results = this.queryView(DRUG_BY_PATIENT_ID_VIEW, patientIds, null);

		Map<String,List<Medication>> medications = new HashMap<String,List<Medication>>();

		for (Map<String, Object> row : (List<Map<String,Object>>)MapUtils.get("rows", results)){
			String patientId = MapUtils.get("value.patientId", row).toString();
			String code = MapUtils.get("value.clinicalDrug.code", row).toString();

			String startDate = null;
			if(MapUtils.keyExists("value.startTime", row)){
				startDate = MapUtils.get("value.startTime", row).toString();
			}
			String endDate = null;
			if(MapUtils.keyExists("value.endTime", row)){
				Object dateFromCouchDb = MapUtils.get("value.endTime", row);
				if(dateFromCouchDb != null){
					endDate = dateFromCouchDb.toString();
				}	
			}
			
			Medication medication;

				if(startDate==null){
					throw new RuntimeException("StartDate should never be null.");
				}
				if(endDate==null){
					//if the returning date is null, we just generate a temporary date
					endDate = "20110224111900";
				}
            try {
				medication = new Medication(
						new Concept(code, CemDbUtils.RXNORM, null),
                        MedicationStatus.ACTIVE,
						CemDbUtils.CEMDB_DATE_FORMAT2.parse(startDate),
						CemDbUtils.CEMDB_DATE_FORMAT2.parse(endDate));
			} catch (ParseException e) {
                log.warn(e);
                continue;
			}

			if(!medications.containsKey(patientId)){
				medications.put(patientId, new ArrayList<Medication>());
			}
			medications.get(patientId).add(medication);

		}	
		return medications;
	}
	
	/**
	 * Query view.
	 *
	 * @param viewName the view name
	 * @param keys the keys
	 * @param params the params
	 * @return the map
	 */
	private Map<String, Object> queryView(String viewName,  Collection<String> keys, Map<String,String> params) {
		return this.couchDbClient.queryView(viewName, keys, params);
	}

	
	/*
	 * Below are the CouchDB views. These are currently loaded into CouchDB,
	 * but are listed here for reference.
	 */
	/*
	 function(doc) {
		  var patient = {};
		  var id = doc.SecondaryUsePatient.patientExternalId[0].ii.extension.value;
		  var gender = doc.SecondaryUsePatient.administrativeGender.cd.originalText.value;
		  var race = doc.SecondaryUsePatient.administrativeRace.cd.originalText.value;
		  var birthDate = doc.SecondaryUsePatient.birthDate.ts.originalText.value;
		
		  patient.id = id;
		  patient.race = race;
		  patient.gender = gender;
		  patient.birthDate = birthDate;
		 
		  emit(id, patient);
     }
    */
	
	/*
	 function(doc) {
		  var patientId = doc.SecondaryUseNotedDrug.patientExternalId[0].ii.extension.value;
		  
		  var drug = {};
		  drug.clinicalDrug = {};
		
		  var drugCode = doc.SecondaryUseNotedDrug.clinicalDrug.cd.code.value.value;
		  var drugCodeSystem = doc.SecondaryUseNotedDrug.clinicalDrug.cd.codeSystem.value.value;
		  
		  drug.patientId = patientId;
		  drug.clinicalDrug.code = drugCode;
		  drug.clinicalDrug.codeSystem = drugCodeSystem;
		 
		  emit(id, drug);
		}
	 */

}