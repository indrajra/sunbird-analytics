package org.ekstep.analytics.vidyavaani.job

import org.ekstep.analytics.framework.IJob
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.JobContext
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.DataNode
import org.ekstep.analytics.framework.util.GraphDBUtil
import org.apache.commons.lang3.StringUtils
import org.ekstep.analytics.framework.RelationshipDirection
import org.ekstep.analytics.framework.Relation
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.dispatcher.GraphQueryDispatcher
import org.ekstep.analytics.job.IGraphExecutionModel
import org.apache.spark.rdd.RDD
import scala.collection.JavaConversions._

object AuthorRelationsModel extends IGraphExecutionModel with Serializable {

    val NODE_NAME = "User";
    val CONTENT_AUTHOR_RELATION = "createdBy"

    val DELETE_AUTHOR_QUERY = "MATCH(ee:User{type: 'author'}) DETACH DELETE ee"
    //val INDEX_QUERY = "CREATE INDEX ON :User(type)"
    val CONTENT_AUTHOR_REL_QUERY = "MATCH (c:domain{IL_FUNC_OBJECT_TYPE:'Content'}), (u:User{type:'author'}) WHERE u.IL_UNIQUE_ID = c.portalOwner CREATE (c)-[r:createdBy]->(u) RETURN r"
    //val AUTHOR_CONCEPT_REL_QUERY = "MATCH (A:User {type:'author'}), (C:domain{IL_FUNC_OBJECT_TYPE:'Concept'}) OPTIONAL MATCH path = (C)<-[:associatedTo]-(f:domain{IL_FUNC_OBJECT_TYPE:'Content',status:'Live'})-[:createdBy]->(A) WITH A, C, CASE WHEN path is null THEN 0 ELSE COUNT(path) END AS overlap MERGE (C)-[:usedBy{support:overlap}]->(A)"
    val AUTHOR_CONCEPT_REL_QUERY = "MATCH (usr :User {type:'author'}), (cnc :domain{IL_FUNC_OBJECT_TYPE:'Concept'}) OPTIONAL MATCH path = (usr)<-[:createdBy]-(cnt:domain{IL_FUNC_OBJECT_TYPE:'Content'})-[nc:associatedTo]->(cnc) WHERE lower(cnt.contentType) IN ['story', 'game', 'collection', 'worksheet'] AND cnt.status IN ['Draft', 'Review', 'Live'] WITH cnc, usr, CASE WHEN path is null THEN 0 ELSE COUNT(path) END AS cc MERGE (cnc)<-[r:uses{contentCount: cc}]-(usr) RETURN r"
    val SET_CONTENT_COUNT_IN_AUTHOR = "match (usr: User {type:'author'}) <- [r:createdBy] - (cnt: domain{IL_FUNC_OBJECT_TYPE:'Content'}) WHERE lower(cnt.contentType) IN ['story', 'game', 'collection', 'worksheet'] WITH usr,count(cnt) as rels SET usr.contentCount = rels"
    val SET_LIVE_CONTENT_COUNT_IN_AUTHOR = "match (usr: User {type:'author'}) <- [r:createdBy] - (cnt: domain{IL_FUNC_OBJECT_TYPE:'Content', status:'Live'}) WHERE lower(cnt.contentType) IN ['story', 'game', 'collection', 'worksheet'] WITH usr,count(cnt) as rels SET usr.liveContentCount = rels"
    
    override def name(): String = "ContentLanguageRelationModel";
    override implicit val className = "org.ekstep.analytics.vidyavaani.job.AuthorRelationsModel"

    override def preProcess(input: RDD[String], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[String] = {
        sc.parallelize(Seq(DELETE_AUTHOR_QUERY), JobContext.parallelization);
    }

    override def algorithm(ppQueries: RDD[String], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[String] = {

        val contentNodes = GraphDBUtil.findNodes(Map("IL_FUNC_OBJECT_TYPE" -> "Content"), Option(List("domain")));

        val authorNodes = contentNodes.map { x => x.metadata.getOrElse(Map()) }
            .map(f => (f.getOrElse("portalOwner", "").asInstanceOf[String], f.getOrElse("owner", "").asInstanceOf[String]))
            .groupBy(f => f._1).filter(p => !StringUtils.isBlank(p._1))
            .map { f =>
                val identifier = f._1;
                val namesList = f._2.filter(p => !StringUtils.isBlank(p._2));
                val name = if (namesList.isEmpty) identifier else namesList.last._2;
                DataNode(identifier, Option(Map("name" -> name, "type" -> "author")), Option(List(NODE_NAME)));
            }

        val authorQuery = GraphDBUtil.createNodesQuery(authorNodes)
        ppQueries.union(sc.parallelize(Seq(authorQuery, CONTENT_AUTHOR_REL_QUERY, AUTHOR_CONCEPT_REL_QUERY, SET_CONTENT_COUNT_IN_AUTHOR, SET_LIVE_CONTENT_COUNT_IN_AUTHOR), JobContext.parallelization));
    }
}