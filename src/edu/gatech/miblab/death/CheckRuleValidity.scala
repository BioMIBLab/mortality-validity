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
 * Rules files shall be CSV files of the format:
 * 		n, r1, r2, [r3, ...]
 * 
 * Valid code files shall be CSV files of the format:
 * 		code, details, [...]
 * 
 * Ontology files shall be CSV files of the format:
 * 		address, subaddress_min, subaddress_max, ambivalence, [...]
 * 
 * Notes:
 * 	- If subaddress_max is an empty string, it is interpreted as a
 * 		1:1 mapping of subaddress_min causes address
 * 	- Ambivalence may be either an empty string or "M"
 *  - Code validity details will be read only as the word "valid" or "invalid"
 * 
 * @author Ryan Hoffman
 * 
 */
object CheckRuleValidity extends App {
  
  val INCLUDE_AMBIVALENT = true  // include relationships marked ambivalently 
                                 // acceptable in the ontology file?
  val tic = System.nanoTime()
  
  // open files
  val rulesFile = io.Source.fromFile("data/rules.csv")
  val codesFile = io.Source.fromFile("data/valid_codes_2015.csv")
  val ontologyFile = io.Source.fromFile("data/Causal2015semi.csv")
  
  // read in the ontology of valid relationships
  // map address -> List[(subaddress_min, subaddress_max)]
  print("Importing ontology file... ")
  val ontology = new HashMap[String, Set[(String,String)]]()
  for (line <- ontologyFile.getLines()) {
    var cols = line.split(',')
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
  ontologyFile.close()
  println("done - " + ontology.size + " addresses with one or more subaddresses")
  
  // read in the list of all valid codes
  // TODO ask if anything else is needed with this data set
  print("Importing valid codes file... ")
  val valid = new HashSet[String]()
  for (line <- codesFile.getLines()) {
    var cols = line.split(',')
    if (cols(1).substring(0,5) == "Valid") valid.add(cols(0));
  }
  codesFile.close()
  println("done - " + valid.size + " valid codes in the codes list")
  
  // read in the rules
  print("Importing rules file... ")
  case class Rule(support: Int, events: Seq[String])
  val rules = (for (line <- rulesFile.getLines()) yield {
    line.split(",").toList match {
      case n :: seq => Rule(n.toInt, seq)
      case _        => throw new Exception() // should never happen
    }
  }).toList
  rulesFile.close()
  println("done - " + rules.length + " rules read in")
  
  // filter down to just invalid rules
  print("Checking rules... ")
  val invalidRules = rules.filterNot({r => validCodes(r) & validCauses(r)})
  println("done - " + invalidRules.length + " invalid rules found")
  
  // write the invalids to a file
  print("Writing output to output/invalid.csv... ");
  val pw = new PrintWriter(new File("output/invalid.csv"))
  pw.println("Support,\"Rule (r1->r2->...)\"")
  invalidRules.foreach(i => {
    pw.println(i.support + "," + i.events.mkString(","))
  })
  pw.close()
  println("done")
  
  val toc = System.nanoTime()
  println("CheckRuleValidity complete: " + (toc-tic)/1e9 + "s elapsed")
  
  /**
   * Check if a rule conforms to the ontology
   * @param a rule
   * @return is the rule valid
   */
  def validCauses(rule: Rule): Boolean = {
    val rels = rule.events.sliding(2); // break a long sequence into pairs
    rels.map({
      case cause :: effect :: Nil => {
        ontology.get(effect) match {
          case Some(x) => (for ((min,max) <- x) yield 
                             (min<=cause && cause<=max)).reduce(_|_)
          case None    => false
        }
      }
      case _ => throw new Exception()
    }).reduce(_&_)
  }
  
  /**
   * Check if a rule's list of codes are all reportable
   * @param a rule
   * @return are all codes valid
   */
  def validCodes(rule: Rule): Boolean = {
    rule.events.map(valid.contains(_)).reduce(_&_)
  }
  
}