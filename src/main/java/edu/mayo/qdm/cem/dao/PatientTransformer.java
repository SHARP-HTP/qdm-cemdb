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

import edu.mayo.qdm.patient.Gender;
import edu.mayo.qdm.patient.Patient;
import edu.mayo.qdm.patient.Race;

import java.text.ParseException;
import java.util.Date;
import java.util.Map;


/**
 * A class responsible for transforming the CemDB JSON representation of a Patient 
 * into a Patient object.
 *
 * @author <a href="mailto:kevin.peterson@mayo.edu">Kevin Peterson</a>
 */
public class PatientTransformer implements CouchDbIterator.Transformer<Patient> {
	
	private final static String BLACK = "Black or African American";
	private final static String WHITE = "White";
	private final static String AFRICAN = "African";
	private final static String UNKNOWN = "Unknown";
	private final static String ASIAN = "Asian";
	private final static String OTHER = "Other";
	private final static String ASIAN_INDIAN = "Asian Indian";
	private final static String NO_DISCLOSURE = "Choose Not to Disclose";
	
	/* (non-Javadoc)
	 * @see edu.mayo.bmi.phenotyping.datasource.impl.cem.dao.CouchDbIterator.Transformer#transform(java.util.Map)
	 */
	@Override
	public Patient transform(Map<String, Object> row) {
		String patientId = MapUtils.get("value.id", row).toString();
		Patient patient = new Patient(patientId);
		
		String birthDate = MapUtils.get("value.birthDate", row).toString();
		try {
			Date birth = CemDbUtils.CEMDB_DATE_FORMAT1.parse(birthDate);
			
			patient.setBirthdate(birth);
		} catch (ParseException e) {
			throw new RuntimeException(e);
		}

		String gender = MapUtils.get("value.gender.code", row).toString();
		if(gender.equalsIgnoreCase("F")){
			patient.setSex(Gender.FEMALE);
		} else if(gender.equalsIgnoreCase("M")){
			patient.setSex(Gender.MALE);
		} else {
			throw new IllegalStateException();
		}
		
		String race = MapUtils.get("value.race", row).toString();
		patient.setRace(this.parseRace(race));
		
		return patient;
	}
	
	private Race parseRace(String race){
		if(race.equals(ASIAN)){
			return Race.ASIAN;
		} else if(race.equals(WHITE)){
			return Race.WHITE;
		} else if(race.equals(AFRICAN)){
			return Race.OTHER;
		} else if(race.equals(BLACK)){
			return Race.BLACKORAFRICANAMERICAN;
		} else if(race.equals(UNKNOWN)){
			return Race.UNKNOWN;
		} else if(race.equals(OTHER)){
			return Race.OTHER;
		} else if(race.equals(ASIAN_INDIAN)){
			return Race.ASIANINDIAN;
		} else if(race.equals(NO_DISCLOSURE)){
			return Race.OTHER;
		} else {
			throw new IllegalStateException("Unrecognized Race: " + race);
		}
	}
}
