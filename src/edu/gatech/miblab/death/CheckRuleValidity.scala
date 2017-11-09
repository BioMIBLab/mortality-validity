package edu.gatech.miblab.death

import io.Source
import collection.mutable.HashMap
import collection.mutable.HashSet

import java.io.File
import java.io.PrintWriter

/**
 * Check if mined rules are valid according to the 
 * ontology files provided by the CDC.
 * 
 * @author Ryan Hoffman
 */
object CheckRuleValidity extends App {
  
  val tic = System.nanoTime()
  val DEBUG = true
  
  // load source data
  val rules = loadRules("data/rules.csv")
  val codes = loadCodes("data/valid_codes_2015.csv")
  val causes = loadValidCauses("data/Causal2015semi.csv")
  val icd10 = loadICD10map("data/icd10cm.csv")
  
  // filter down to just invalid rules
  print("Checking rules... ")
  val codesValid = validCodesChecker(codes)(_)
  val orderValid = validCausesChecker(causes)(_)
  val invalidRules = rules.filterNot({r => codesValid(r) && orderValid(r)})
  println("done - " + invalidRules.size + " invalid rules found")
  
  // write the invalids to a file
  print("Writing output to output/invalid.csv... ")
  val icd10desc = ruleToICD10String(icd10)(_)
  val output = new PrintWriter(new File("output/invalid.csv"))
  output.println("Support,Rule,Codes (r1->r2->...)")
  invalidRules.foreach(i => {
    output.println(i.support + ",\"" + icd10desc(i) + "\"," + 
                   i.events.mkString(","))
  })
  output.close()
  println("done")
  
  val toc = System.nanoTime()
  println("CheckRuleValidity complete: " + (toc-tic)/1e9 + "s elapsed")
  
  /**
   * Check if a rule conforms to the ontology
   * @param a rule
   * @return is the rule valid
   */
  def validCausesChecker(ontology: HashMap[String, Set[(String,String)]])
                        (rule: Rule): Boolean = {
    // break a long sequence into pairs, check each pair
    rule.events.sliding(2).map({
      case cause :: effect :: Nil => {
        ontology.get(effect) match {
          case Some(x) => (for ((min,max) <- x) yield 
                             (min<=cause && cause<=max)).reduce(_|_)
          case None    => false
        }
      }
      case _ => throw new Exception(rule.toString) // should never happen
    }).reduce(_&_)
  }
  
  /**
   * Check if a rule's list of codes are all reportable
   * @param a rule
   * @return are all codes valid
   */
  def validCodesChecker(valid: HashSet[String])(rule: Rule): Boolean = {
    rule.events.map(valid.contains(_)).reduce(_&_)
  }
  
  def ruleToICD10String(mapping: HashMap[String,String])(rule: Rule): String = {
    rule.events.map(c => mapping.get(c) match {
      case Some(x) => x
      case None    => c
    }).mkString(" -> ")
  }
  
  /**
   * Load in SPM rules file
   * 
   * Rules files shall be CSV files of the format:
   * 		n, r1, r2, [r3, ...]
   */
  def loadRules(rulesFileName: String): List[Rule] = {
    val rulesFile = io.Source.fromFile(rulesFileName)
		print("Importing rules file... ")
    val rules = (for (line <- rulesFile.getLines()) yield {
	    val cols = line.split(",")
	    Rule(cols.head.toInt, cols.tail.toList)
    }).toList
    rulesFile.close()
    println("done - " + rules.size + " rules read in")
    return rules
  }
  
  /**
   * Load in the list of all reportable codes (ACME Tables A-C)
   * 
   * Valid code files shall be CSV files of the format:
   * 		code, details, [...]
   * 
   * TODO ask if anything else is needed with this data set
   */
  def loadCodes(codesFileName: String): HashSet[String] = {
    print("Importing valid codes file... ")
    val codesFile = io.Source.fromFile(codesFileName)
    val valid = new HashSet[String]()
    for (line <- codesFile.getLines()) {
      val cols = line.split(',')
      if (cols(1).substring(0,5) == "Valid") valid.add(cols(0))
    }
    codesFile.close()
    println("done - " + valid.size + " valid codes in the codes list")
    return valid
  }
  
  /**
   * Load in the ontology outlining all valid causal relationships (ACME Table D)
   * 
   * Ontology files shall be CSV files of the format:
 	 * 		address, subaddress_min, subaddress_max, ambivalence, [...]
 	 * 
 	 * Notes:
   * 	- If subaddress_max is an empty string, it is interpreted as a
   * 		1:1 mapping of subaddress_min causes address
   * 	- Ambivalence may be either an empty string or "M"
   */
  def loadValidCauses(causesFileName: String): HashMap[String, Set[(String,String)]] = {
    print("Importing ontology file... ")
    val INCLUDE_AMBIVALENT = true
    val causesFile = io.Source.fromFile(causesFileName)
    // map address -> List[(subaddress_min, subaddress_max)]
    val ontology = new HashMap[String, Set[(String,String)]]()
    for (line <- causesFile.getLines()) {
      val cols = line.split(',')
      if (cols.length>=4 && (!cols(3).equals("M") | INCLUDE_AMBIVALENT)) {
        // if only a single subaddress present, use as both min and max
        val addr = cols(0)
        val subaddr = (cols(1), if(cols(2).isEmpty()) cols(1) else cols(2))
        ontology.get(addr) match {
          case None    => ontology += addr -> Set(subaddr)
          case Some(x) => ontology += addr -> (x + subaddr) 
        }
      }
    }
    causesFile.close()
    println("done - " + ontology.size + " addresses with one or more subaddresses")
    return ontology
  }
  
  /**
   * Load in the mapping of ICD-10 codes to human-readable descriptions
   * 
   * ICD-10 file shall be CSV files of the format:
   * 		code, description
   */
  def loadICD10map(icd10FileName: String): HashMap[String,String] = {
    print("Importing ICD-10 ... ")
    val INCLUDE_AMBIVALENT = true
    val icd10File = io.Source.fromFile(icd10FileName)
    val icd10 = new HashMap[String, String]()
    for (line <- icd10File.getLines()) {
      val cols = line.split(',')
      icd10 += cols.head -> cols.tail.mkString(",").replaceAll("\"", "")
    }
    icd10File.close()
    println("done - " + icd10.size + " code descriptions imported")
    return icd10
  }
  
  case class Rule(support: Int, events: Seq[String])
  
}