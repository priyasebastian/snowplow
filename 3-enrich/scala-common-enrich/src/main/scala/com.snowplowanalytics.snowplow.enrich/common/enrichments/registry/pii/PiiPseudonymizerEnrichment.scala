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
package pii

// Scala
import scala.collection.JavaConverters._
import scala.collection.mutable.MutableList

// Scala libraries
import org.json4s
import org.json4s.{DefaultFormats, JValue}
import org.json4s.JsonAST._
import org.json4s.jackson.JsonMethods
import org.json4s.jackson.JsonMethods.{compact, parse, render}
import org.json4s.jackson.Serialization.write

// Java
import java.security.{MessageDigest, NoSuchAlgorithmException}

// Java libraries
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode, TextNode}
import com.jayway.jsonpath.{Configuration, JsonPath => JJsonPath}
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

/**
 * Companion object. Lets us create a PiiPseudonymizerEnrichment
 * from a JValue.
 */
object PiiPseudonymizerEnrichment extends ParseableEnrichment {

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
            .getOrElse(s"The specified json field ${fieldName} is not supported".failure)) |@|
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
 * Implements a pseudonymization strategy using any algorithm known to MessageDigest
 * @param hashFunction the MessageDigest function to apply
 */
final case class PiiStrategyPseudonymize(hashFunction: MessageDigest) extends PiiStrategy {
  val TextEncoding                                 = "UTF-8"
  override def scramble(clearText: String): String = hash(clearText)
  def hash(text: String): String =
    String.format("%064x", new java.math.BigInteger(1, hashFunction.digest(text.getBytes(TextEncoding))))
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
  implicit val json4sFormats = DefaultFormats + new PiiModifiedFieldsSerializer + new PiiStrategySerializer
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

private final case class ScrambleMapFunction(strategy: PiiStrategy,
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
