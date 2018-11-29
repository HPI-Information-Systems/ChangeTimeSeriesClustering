package de.hpi.data_change.time_series_similarity.dba

import de.hpi.data_change.time_series_similarity.ClusteringAlg
import org.json4s.JsonAST.JInt
import org.json4s.{CustomSerializer, JField, JObject, JString}

class ClusteringAlgSerializer extends CustomSerializer[ClusteringAlg](format => (
    {
        case JObject(
        JField("name", JString(name))
            :: JField("k", JInt(k))
            :: JField("maxIter", JInt(maxIter))
            :: JField("seed", JInt(seed))
            :: Nil
        ) => ClusteringAlg(name, k.toInt, maxIter.toInt, seed.toLong)
    },
    {
        case address: ClusteringAlg =>
            JObject(
                JField("name", JString(address.name))
                    :: JField("k", JInt(address.k))
                    :: JField("maxIter", JInt(address.maxIter))
                    :: JField("seed", JInt(address.seed))
                    :: Nil
            )
    }
))
