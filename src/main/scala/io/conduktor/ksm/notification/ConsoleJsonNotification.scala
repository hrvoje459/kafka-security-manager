package io.conduktor.ksm.notification

import com.typesafe.config.Config
import io.circe.Json
import io.conduktor.ksm.parser.csv.CsvParserException
import io.conduktor.ksm.parser.yaml.YamlParserException
import kafka.security.auth.{Acl, Resource}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}

case class ConsoleJsonNotification() extends Notification {

  val log: Logger =
    LoggerFactory.getLogger(classOf[ConsoleJsonNotification].getSimpleName)

  /**
    * Config Prefix for configuring this module
    */
  override val CONFIG_PREFIX: String = "console"

  /**
    * internal config definition for the module
    */
  override def configure(config: Config): Unit = ()

  override def notifyErrors(errs: List[Try[Throwable]]): Unit = {
    errs.foreach {
      case Failure(cPE: CsvParserException) =>
        log.error(s"${cPE.getLocalizedMessage} | Row: ${cPE.printRow()}")
      case Failure(cPE: YamlParserException) =>
        log.error(s"${cPE.getLocalizedMessage} | Detail: ${cPE.print()}")
      case Success(t) => log.error("refresh exception", t)
      case Failure(t) => log.error("refresh exception", t)
    }
  }

  override protected def notifyOne(
                                    action: String,
                                    acls: Set[(Resource, Acl)]
                                  ): Unit = {
    if (acls.nonEmpty) {
      acls.foreach {
        case (resource, acl) =>
          val message = Notification.printAcl(acl, resource)
          log.info(s"$action $message")
      }
    } else {
      log.info(s"No changes $action")
    }
  }

  override protected def notifyBoth(
      addedAcls: Set[(Resource, Acl)],
      removedAcls: Set[(Resource, Acl)]
  ): Unit = {
    if (addedAcls.nonEmpty || removedAcls.nonEmpty) {
      /*acls.foreach {
        case (resource, acl) =>
          val message = Notification.printAcl(acl, resource)
          log.info(s"$action $message")
      }*/

      log.info(addedAcls.size.toString())
      val arrayOfAcls = new ArrayBuffer[Json]()
      log.info(arrayOfAcls.toString())

      addedAcls.foreach {
        case (resource, acl) =>
          val resourceJson = Json.obj(
            "resource_type" -> Json.fromString(resource.resourceType.toString),
            "resource_name" -> Json.fromString(resource.name),
            "pattern_type" -> Json.fromString(resource.patternType.toString)
          )
          val principalJson = Json.obj(
            "principal_type" -> Json.fromString(acl.principal.getPrincipalType),
            "principal_name" -> Json.fromString(acl.principal.getName)
          )
          val aclJson = Json.obj(
            "action" -> Json.fromString("ADD"),
            "principal" -> Json.fromJsonObject(principalJson.asObject.get),
            "operation" -> Json.fromString(acl.operation.toString),
            "permission_type" -> Json.fromString(acl.permissionType.toString),
            "host" -> Json.fromString(acl.host),
            "resource" -> Json.fromJsonObject(resourceJson.asObject.get)
          )
          arrayOfAcls.append(aclJson)
          //log.info("JSON OBJECT: " + aclJson.toString())
          //val message = Notification.printAcl(acl, resource)
          //val messageJson = message.split(",").asJson
          //log.info(s"$action $messageJson")
      }

      removedAcls.foreach {
        case (resource, acl) =>
          val resourceJson = Json.obj(
            "resource_type" -> Json.fromString(resource.resourceType.toString),
            "resource_name" -> Json.fromString(resource.name),
            "pattern_type" -> Json.fromString(resource.patternType.toString)
          )
          val principalJson = Json.obj(
            "principal_type" -> Json.fromString(acl.principal.getPrincipalType),
            "principal_name" -> Json.fromString(acl.principal.getName)
          )
          val aclJson = Json.obj(
            "action" -> Json.fromString("REMOVE"),
            "principal" -> Json.fromJsonObject(principalJson.asObject.get),
            "operation" -> Json.fromString(acl.operation.toString),
            "permission_type" -> Json.fromString(acl.permissionType.toString),
            "host" -> Json.fromString(acl.host),
            "resource" -> Json.fromJsonObject(resourceJson.asObject.get)
          )
          arrayOfAcls.append(aclJson)
        //log.info("JSON OBJECT: " + aclJson.toString())
        //val message = Notification.printAcl(acl, resource)
        //val messageJson = message.split(",").asJson
        //log.info(s"$action $messageJson")
      }


      println(arrayOfAcls.mkString(", "))
      //println("HRVOJE NOT LOGING " + arrayOfAcls.mkString(", ")
    } else {
      log.info(s"No changes to ADD or REMOVE")
      new ArrayBuffer[Json]()
    }
  }

  override def close(): Unit = ()

}
