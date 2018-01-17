/*
 * Copyright (c) 2017-2018 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */

package com.snowplowanalytics
package snowplow.enrich
package common.enrichments.registry

// Scala
import scala.collection.JavaConverters._
import scala.collection.mutable.MutableList

// Scala libraries
import org.json4s
import org.json4s.{CustomSerializer, DefaultFormats, JValue}
import org.json4s.JsonAST._
import org.json4s.JsonDSL._
import org.json4s.Extraction.decompose
import org.json4s.jackson.JsonMethods
import org.json4s.jackson.JsonMethods.{compact, parse, render}
import org.json4s.jackson.Serialization.write

// Java
import java.security.{MessageDigest, NoSuchAlgorithmException}

// Java libraries
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode, TextNode}
import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider
import com.jayway.jsonpath.{Configuration, JsonPath => JJsonPath, Option => JOption}
import com.jayway.jsonpath.MapFunction

// Scalaz
import scalaz._
import Scalaz._

// Iglu
import iglu.client.validation.ProcessingMessageMethods._
import iglu.client.{SchemaCriterion, SchemaKey}

// This project
import common.ValidatedNelMessage
import common.utils.ScalazJson4sUtils
import common.outputs.EnrichedEvent

object PiiConstants {
  type ModifiedFields  = List[ModifedField]
  type ApplyStartegyFn = (String, PiiStrategy) => (String, ModifiedFields)
  type MutatorFn       = (EnrichedEvent, PiiStrategy, ApplyStartegyFn) => ModifiedFields

  /**
   * This and the next constant maps from a configuration field name to an EnrichedEvent mutator. The structure is such so that
   * it preserves type safety, and it can be easily replaced in the future by generated code that will use the configuration as
   * input.
   */
  val ScalarMutators: Map[String, Mutator] = Map(
    "user_id" -> Mutator(
      "user_id", { (event: EnrichedEvent, strategy: PiiStrategy, fn: ApplyStartegyFn) =>
        val (newValue, modifiedFields) = fn(event.user_id, strategy)
        event.user_id = newValue
        modifiedFields
      }
    ),
    "user_ipaddress" -> Mutator(
      "user_ipaddress", { (event: EnrichedEvent, strategy: PiiStrategy, fn: ApplyStartegyFn) =>
        val (newValue, modifiedFields) = fn(event.user_ipaddress, strategy)
        event.user_ipaddress = newValue
        modifiedFields
      }
    ),
    "user_fingerprint" -> Mutator(
      "user_fingerprint", { (event: EnrichedEvent, strategy: PiiStrategy, fn: ApplyStartegyFn) =>
        val (newValue, modifiedFields) = fn(event.user_fingerprint, strategy)
        event.user_fingerprint = newValue
        modifiedFields
      }
    ),
    "domain_userid" -> Mutator(
      "domain_userid", { (event: EnrichedEvent, strategy: PiiStrategy, fn: ApplyStartegyFn) =>
        val (newValue, modifiedFields) = fn(event.domain_userid, strategy)
        event.domain_userid = newValue
        modifiedFields
      }
    ),
    "network_userid" -> Mutator(
      "network_userid", { (event: EnrichedEvent, strategy: PiiStrategy, fn: ApplyStartegyFn) =>
        val (newValue, modifiedFields) = fn(event.network_userid, strategy)
        event.network_userid = newValue
        modifiedFields
      }
    ),
    "ip_organization" -> Mutator(
      "ip_organization", { (event: EnrichedEvent, strategy: PiiStrategy, fn: ApplyStartegyFn) =>
        val (newValue, modifiedFields) = fn(event.ip_organization, strategy)
        event.ip_organization = newValue
        modifiedFields
      }
    ),
    "ip_domain" -> Mutator(
      "ip_domain", { (event: EnrichedEvent, strategy: PiiStrategy, fn: ApplyStartegyFn) =>
        val (newValue, modifiedFields) = fn(event.ip_domain, strategy)
        event.ip_domain = newValue
        modifiedFields
      }
    ),
    "tr_orderid" -> Mutator(
      "tr_orderid", { (event: EnrichedEvent, strategy: PiiStrategy, fn: ApplyStartegyFn) =>
        val (newValue, modifiedFields) = fn(event.tr_orderid, strategy)
        event.tr_orderid = newValue
        modifiedFields
      }
    ),
    "ti_orderid" -> Mutator(
      "ti_orderid", { (event: EnrichedEvent, strategy: PiiStrategy, fn: ApplyStartegyFn) =>
        val (newValue, modifiedFields) = fn(event.ti_orderid, strategy)
        event.ti_orderid = newValue
        modifiedFields
      }
    ),
    "mkt_term" -> Mutator(
      "mkt_term", { (event: EnrichedEvent, strategy: PiiStrategy, fn: ApplyStartegyFn) =>
        val (newValue, modifiedFields) = fn(event.mkt_term, strategy)
        event.mkt_term = newValue
        modifiedFields
      }
    ),
    "mkt_content" -> Mutator(
      "mkt_content", { (event: EnrichedEvent, strategy: PiiStrategy, fn: ApplyStartegyFn) =>
        val (newValue, modifiedFields) = fn(event.mkt_content, strategy)
        event.mkt_content = newValue
        modifiedFields
      }
    ),
    "se_category" -> Mutator(
      "se_category", { (event: EnrichedEvent, strategy: PiiStrategy, fn: ApplyStartegyFn) =>
        val (newValue, modifiedFields) = fn(event.se_category, strategy)
        event.se_category = newValue
        modifiedFields
      }
    ),
    "se_action" -> Mutator(
      "se_action", { (event: EnrichedEvent, strategy: PiiStrategy, fn: ApplyStartegyFn) =>
        val (newValue, modifiedFields) = fn(event.se_action, strategy)
        event.se_action = newValue
        modifiedFields
      }
    ),
    "se_label" -> Mutator(
      "se_label", { (event: EnrichedEvent, strategy: PiiStrategy, fn: ApplyStartegyFn) =>
        val (newValue, modifiedFields) = fn(event.se_label, strategy)
        event.se_label = newValue
        modifiedFields
      }
    ),
    "se_property" -> Mutator(
      "se_property", { (event: EnrichedEvent, strategy: PiiStrategy, fn: ApplyStartegyFn) =>
        val (newValue, modifiedFields) = fn(event.se_property, strategy)
        event.se_property = newValue
        modifiedFields
      }
    ),
    "mkt_clickid" -> Mutator(
      "mkt_clickid", { (event: EnrichedEvent, strategy: PiiStrategy, fn: ApplyStartegyFn) =>
        val (newValue, modifiedFields) = fn(event.mkt_clickid, strategy)
        event.mkt_clickid = newValue
        modifiedFields
      }
    ),
    "refr_domain_userid" -> Mutator(
      "refr_domain_userid", { (event: EnrichedEvent, strategy: PiiStrategy, fn: ApplyStartegyFn) =>
        val (newValue, modifiedFields) = fn(event.refr_domain_userid, strategy)
        event.refr_domain_userid = newValue
        modifiedFields
      }
    ),
    "domain_sessionid" -> Mutator(
      "domain_sessionid", { (event: EnrichedEvent, strategy: PiiStrategy, fn: ApplyStartegyFn) =>
        val (newValue, modifiedFields) = fn(event.domain_sessionid, strategy)
        event.domain_sessionid = newValue
        modifiedFields
      }
    )
  )

  val JsonMutators: Map[String, Mutator] = Map(
    "contexts" -> Mutator(
      "contexts", { (event: EnrichedEvent, strategy: PiiStrategy, fn: ApplyStartegyFn) =>
        val (newValue, modifiedFields) = fn(event.contexts, strategy)
        event.contexts = newValue
        modifiedFields
      }
    ),
    "derived_contexts" -> Mutator(
      "derived_contexts", { (event: EnrichedEvent, strategy: PiiStrategy, fn: ApplyStartegyFn) =>
        val (newValue, modifiedFields) = fn(event.derived_contexts, strategy)
        event.derived_contexts = newValue
        modifiedFields
      }
    ),
    "unstruct_event" -> Mutator(
      "unstruct_event", { (event: EnrichedEvent, strategy: PiiStrategy, fn: ApplyStartegyFn) =>
        val (newValue, modifiedFields) = fn(event.unstruct_event, strategy)
        event.unstruct_event = newValue
        modifiedFields
      }
    )
  )
}

/**
 * PiiField trait. This corresponds to a configuration top-level field (i.e. either a scalar or a JSON field) along with
 * a function to apply that strategy to the EnrichedEvent POJO (A scalar field is represented in config py "pojo")
 */
sealed trait PiiField {
  import PiiConstants.ModifiedFields

  /**
   * The POJO mutator for this field
   *
   * @return fieldMutator
   */
  def fieldMutator: Mutator

  /**
   * Gets an enriched event from the enrichment manager and modifies it according to the specified strategy.
   *
   * @param event The enriched event
   */
  def transform(event: EnrichedEvent, strategy: PiiStrategy): ModifiedFields =
    fieldMutator.muatatorFn(event, strategy, applyStrategy)

  protected def applyStrategy(fieldValue: String, strategy: PiiStrategy): (String, ModifiedFields)
}

/**
 * PiiStrategy trait. This corresponds to a strategy to apply to a single field. Currently only String input is
 * supported.
 */
sealed trait PiiStrategy {
  def scramble(clearText: String): String
}

/**
 * The modified field trait represents an item that is transformed in either the JSON or a scalar mutators.
 */
sealed trait ModifedField

/**
 * Companion object. Lets us create a PiiPseudonymizerEnrichment
 * from a JValue.
 */
object PiiPseudonymizerEnrichment extends ParseableEnrichment {
  import PiiConstants._

  implicit val json4sFormats = DefaultFormats

  override val supportedSchema =
    SchemaCriterion("com.snowplowanalytics.snowplow.enrichments", "pii_enrichment_config", "jsonschema", 2, 0, 0)

  def parse(config: JValue, schemaKey: SchemaKey): ValidatedNelMessage[PiiPseudonymizerEnrichment] = {
    for {
      conf <- matchesSchema(config, schemaKey)
      enabled = ScalazJson4sUtils.extract[Boolean](conf, "enabled").toOption.getOrElse(false)
      emitIdentificationEvent = ScalazJson4sUtils
        .extract[Boolean](conf, "emitIdentificationEvent")
        .toOption
        .getOrElse(false)
      piiFields        <- ScalazJson4sUtils.extract[List[JObject]](conf, "parameters", "pii").leftMap(_.getMessage)
      hashFunctionName <- extractStrategyFunction(config)
      hashFunction     <- getHashFunction(hashFunctionName)
      piiFieldList     <- extractFields(piiFields)
    } yield
      if (enabled)
        PiiPseudonymizerEnrichment(piiFieldList, emitIdentificationEvent, PiiStrategyPseudonymize(hashFunction))
      else PiiPseudonymizerEnrichment(List(), emitIdentificationEvent = false, PiiStrategyPseudonymize(hashFunction))
  }.leftMap(_.toProcessingMessageNel)

  private def getHashFunction(strategyFunction: String): Validation[String, MessageDigest] =
    try {
      MessageDigest.getInstance(strategyFunction).success
    } catch {
      case e: NoSuchAlgorithmException =>
        s"Could not parse PII enrichment config: ${e.getMessage}".failure
    }

  private def extractFields(piiFields: List[JObject]): Validation[String, List[PiiField]] =
    piiFields.map {
      case field: JObject =>
        if (ScalazJson4sUtils.fieldExists(field, "pojo"))
          extractString(field, "pojo", "field").flatMap(extractPiiScalarField)
        else if (ScalazJson4sUtils.fieldExists(field, "json")) extractPiiJsonField(field \ "json")
        else s"PII Configuration: pii field does not include 'pojo' nor 'json' fields. Got: [${compact(field)}]".failure
      case json => s"PII Configuration: pii field does not contain an object. Got: [${compact(json)}]".failure
    }.sequenceU

  private def extractPiiScalarField(fieldName: String): Validation[String, PiiScalar] =
    ScalarMutators
      .get(fieldName)
      .map(PiiScalar(_).success)
      .getOrElse(s"The specified pojo field $fieldName is not supported".failure)

  private def extractPiiJsonField(jsonField: JValue): Validation[String, PiiJson] =
    (extractString(jsonField, "field")
      .flatMap(
        fieldName =>
          JsonMutators
            .get(fieldName)
            .map(_.success)
            .getOrElse(s"The specified json field ${compact(jsonField)} is not supported".failure)) |@|
      extractString(jsonField, "schemaCriterion").flatMap(sc => SchemaCriterion.parse(sc).leftMap(_.getMessage)) |@|
      extractString(jsonField, "jsonPath")) { (fieldMutator: Mutator, sc: SchemaCriterion, jsonPath: String) =>
      PiiJson(fieldMutator, sc, jsonPath)
    }

  private def extractString(jValue: JValue, field: String, tail: String*): Validation[String, String] =
    ScalazJson4sUtils.extract[String](jValue, field, tail: _*).leftMap(_.getMessage)

  private def extractStrategyFunction(config: JValue): Validation[String, String] =
    ScalazJson4sUtils
      .extract[String](config, "parameters", "strategy", "pseudonymize", "hashFunction")
      .leftMap(_.getMessage)

  private def matchesSchema(config: JValue, schemaKey: SchemaKey): Validation[String, JValue] =
    if (supportedSchema.matches(schemaKey)) {
      config.success
    } else {
      "Schema key %s is not supported. A '%s' enrichment must have schema '%s'."
        .format(schemaKey, supportedSchema.name, supportedSchema)
        .failure
    }
}

/**
 * The PiiPseudonymizerEnrichment runs after all other enrichments to find fields that are configured as PII (personally
 * identifiable information) and apply some anonymization (currently only pseudonymization) on them. Currently a single
 * strategy for all the fields is supported due to the configuration format, and there is only one implemented strategy,
 * however the enrichment supports a strategy per field.
 *
 * The user may specify two types of fields POJO or JSON. A POJO field is effectively a scalar field in the
 * EnrichedEvent, whereas a JSON is a "context" formatted field and it can be wither a scalar in the case of
 * unstruct_event or an array in the case of derived_events and contexts
 *
 * @param fieldList a list of configured PiiFields
 * @param emitIdentificationEvent whether to emit an identification event
 * @param strategy the pseudonymization strategy to use
 */
case class PiiPseudonymizerEnrichment(fieldList: List[PiiField],
                                      emitIdentificationEvent: Boolean,
                                      strategy: PiiStrategy)
    extends Enrichment {
  import PiiConstants.ModifiedFields
  implicit val json4sFormats = DefaultFormats + new PiiModifiedFieldsSerializer
  def transformer(event: EnrichedEvent): Unit = {
    val modifiedFields: ModifiedFields = fieldList.flatMap(_.transform(event, strategy))
    event.pii = if (modifiedFields.nonEmpty) write(PiiModifiedFields(modifiedFields, strategy)) else null
  }
}

/**
 * Specifies a scalar field in POJO and the strategy that should be applied to it.
 * @param fieldMutator the field mutator where the strategy will be applied
 */
final case class PiiScalar(fieldMutator: Mutator) extends PiiField {
  import PiiConstants.ModifiedFields
  override def applyStrategy(fieldValue: String, strategy: PiiStrategy): (String, ModifiedFields) =
    if (fieldValue != null) {
      val modifiedValue = strategy.scramble(fieldValue)
      (modifiedValue, List(ScalarModifiedField(fieldMutator.fieldName, fieldValue, modifiedValue)))
    } else (null, List())
}

/**
 * Specifies a strategy to use, a field mutator where the JSON can be found in the EnrichedEvent POJO, a schema criterion to
 * discriminate which contexts to apply this strategy to, and a JSON path within the contexts where this strategy will
 * be applied (the path may correspond to multiple fields).
 *
 * @param fieldMutator the field mutator for the JSON field
 * @param schemaCriterion the schema for which the strategy will be applied
 * @param jsonPath the path where the strategy will be applied
 */
final case class PiiJson(fieldMutator: Mutator, schemaCriterion: SchemaCriterion, jsonPath: String) extends PiiField {
  import PiiConstants.ModifiedFields
  implicit val json4sFormats = DefaultFormats

  override def applyStrategy(fieldValue: String, strategy: PiiStrategy): (String, ModifiedFields) = {
    val modifiedFields = MutableList[JsonModifiedField]()
    if (fieldValue != null) {
      (compact(render(parse(fieldValue) match {
        case JObject(jObject) =>
          val jObjectMap = jObject.toMap
          val updated = jObjectMap.filterKeys(_ == "data").mapValues {
            case JArray(contexts) =>
              JArray(contexts.map {
                case JObject(context) =>
                  val (values, listOfModifiedValues) = modifyObjectIfSchemaMatches(context, strategy)
                  modifiedFields ++= listOfModifiedValues
                  values
                case x => x
              })
            case JObject(unstructEvent) =>
              val (values, listOfModifiedValues) = modifyObjectIfSchemaMatches(unstructEvent, strategy)
              modifiedFields ++= listOfModifiedValues
              values
            case x => x
          }
          JObject((jObjectMap ++ updated).toList)
        case x => x
      })), modifiedFields.toList)
    } else (null, modifiedFields.toList)
  }

  /**
   * Tests whether the schema for this event matches the schema criterion and if it does modifies it.
   */
  private def modifyObjectIfSchemaMatches(context: List[(String, json4s.JValue)],
                                          strategy: PiiStrategy): (JObject, List[JsonModifiedField]) = {
    val fieldsObj = context.toMap
    (for {
      schema <- fieldsObj.get("schema")
      schemaStr = schema.extract[String]
      parsedSchemaMatches <- SchemaKey.parse(schemaStr).map(schemaCriterion.matches).toOption
      data                <- fieldsObj.get("data")
      if parsedSchemaMatches
      updated = jsonPathReplace(data, strategy, schemaStr)
    } yield (JObject(fieldsObj.updated("schema", schema).updated("data", updated._1).toList), updated._2))
      .getOrElse((JObject(context), List()))
  }

  // Configuration for JsonPath
  private val JacksonNodeJsonObjectMapper = {
    val objectMapper = new ObjectMapper()
    objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
    objectMapper
  }
  private val JsonPathConf =
    Configuration
      .builder()
      .options(JOption.SUPPRESS_EXCEPTIONS)
      .jsonProvider(new JacksonJsonNodeJsonProvider(JacksonNodeJsonObjectMapper))
      .build()

  /**
   * Replaces a value in the given context data with the result of applying the strategy that value.
   */
  private def jsonPathReplace(jValue: JValue,
                              strategy: PiiStrategy,
                              schema: String): (JValue, List[JsonModifiedField]) = {
    val objectNode      = JsonMethods.mapper.valueToTree[ObjectNode](jValue)
    val documentContext = JJsonPath.using(JsonPathConf).parse(objectNode)
    val modifiedFields  = MutableList[JsonModifiedField]()
    documentContext.map(jsonPath,
                        ScrambleMapFunction(strategy, modifiedFields, fieldMutator.fieldName, jsonPath, schema))
    (JsonMethods.fromJsonNode(documentContext.json[JsonNode]), modifiedFields.toList)
  }
}

case class ScrambleMapFunction(strategy: PiiStrategy,
                               modifiedFields: MutableList[JsonModifiedField],
                               fieldName: String,
                               jsonPath: String,
                               schema: String)
    extends MapFunction {
  override def map(currentValue: AnyRef, configuration: Configuration): AnyRef = currentValue match {
    case s: String =>
      val newValue = strategy.scramble(s)
      modifiedFields += JsonModifiedField(fieldName, s, newValue, jsonPath, schema)
      newValue
    case a: ArrayNode =>
      a.elements.asScala.map {
        case t: TextNode =>
          val originalValue = t.asText()
          val newValue      = strategy.scramble(originalValue)
          modifiedFields += JsonModifiedField(fieldName, originalValue, newValue, jsonPath, schema)
          newValue
        case default: AnyRef => default
      }

    case default: AnyRef => default
  }
}

/**
 * Implements a pseudonymization strategy using any algorithm known to MessageDigest
 * @param hashFunction the MessageDigest function to apply
 */
case class PiiStrategyPseudonymize(hashFunction: MessageDigest) extends PiiStrategy {
  val TextEncoding                                 = "UTF-8"
  override def scramble(clearText: String): String = hash(clearText)
  def hash(text: String): String =
    String.format("%064x", new java.math.BigInteger(1, hashFunction.digest(text.getBytes(TextEncoding))))
}

/**
 * The mutator class encapsulates the mutator function and the field name where the mutator will be applied.
 */
case class Mutator(fieldName: String, muatatorFn: PiiConstants.MutatorFn)

/**
 * Case class for capturing scalar field modifications.
 */
case class ScalarModifiedField(fieldName: String, originalValue: String, modifiedValue: String) extends ModifedField

/**
 * Case class for capturing JSON field modifications.
 */
case class JsonModifiedField(field: String,
                             originalValue: String,
                             modifiedValue: String,
                             jsonPath: String,
                             schema: String)
    extends ModifedField

/**
 * Parent class for classes that serialize the values that were modified during the PII enrichment.
 */
case class PiiModifiedFields(val modifiedFields: PiiConstants.ModifiedFields, val strategy: PiiStrategy)

/**
 * Custom serializer for PiiStrategy class
 */
class PiiStrategySerializer
    extends CustomSerializer[PiiStrategy](format =>
      ({
        case jo: JObject =>
          implicit val json4sFormats = DefaultFormats
          val function               = (jo \ "pseudonymize" \ "hashFunction").extract[String]
          PiiStrategyPseudonymize(MessageDigest.getInstance(function))
      }, {
        case psp: PiiStrategyPseudonymize =>
          "pseudonymize" -> ("hashFunction" -> psp.hashFunction.getAlgorithm)
      }))

/**
 * Custom serializer for PiiModifiedFields class
 */
class PiiModifiedFieldsSerializer
    extends CustomSerializer[PiiModifiedFields](format => {
      val PiiTransformationSchema = "iglu:com.snowplowanalytics.snowplow/pii_transformation/jsonschema/1-0-0"
      ({
        case jo: JObject =>
          implicit val json4sFormats = DefaultFormats + new PiiStrategySerializer
          val fields                 = (jo \ "data" \ "pii").extract[List[ModifedField]]
          val strategy               = (jo \ "data" \ "strategy").extract[PiiStrategy]
          PiiModifiedFields(fields, strategy)
      }, {
        case pmf: PiiModifiedFields =>
          implicit val json4sFormats = DefaultFormats + new PiiStrategySerializer
          ("schema" -> PiiTransformationSchema) ~
            ("data" ->
              ("pii" -> decompose(
                pmf.modifiedFields
                  .map {
                    case s: ScalarModifiedField => "pojo" -> s
                    case j: JsonModifiedField   => "json" -> j
                  }
                  .groupBy(_._1)
                  .mapValues(_.map(_._2))
              ))            ~
                ("strategy" -> decompose(pmf.strategy)))
      })
    })
