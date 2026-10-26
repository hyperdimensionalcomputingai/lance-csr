
        MATCH path = allShortestPaths((person1:Person {ID: $person1Id})-[:knows*0..]-(person2:Person {ID: $person2Id}))
        WITH collect(path) AS paths
        UNWIND paths AS path
        WITH path, relationships(path) AS rels_in_path
        WITH
            [n IN nodes(path) | n.ID] AS personIdsInPath,
            [r IN rels_in_path |
                reduce(w = 0.0, v IN [
                    (a:Person)<-[:commentHasCreator]-(:Comment)-[:replyOfPost]->(:Post)-[:postHasCreator]->(b:Person)
                    WHERE (a.ID = startNode(r).ID AND b.ID = endNode(r).ID) OR (a.ID = endNode(r).ID AND b.ID = startNode(r).ID)
                    | 1.0] | w + v)
            ] AS weight1,
            [r IN rels_in_path |
                reduce(w = 0.0, v IN [
                    (a:Person)<-[:commentHasCreator]-(:Comment)-[:replyOfComment]->(:Comment)-[:commentHasCreator]->(b:Person)
                    WHERE (a.ID = startNode(r).ID AND b.ID = endNode(r).ID) OR (a.ID = endNode(r).ID AND b.ID = startNode(r).ID)
                    | 0.5] | w + v)
            ] AS weight2
        WITH
            personIdsInPath,
            reduce(w = 0.0, v IN weight1 | w + v) AS w1,
            reduce(w = 0.0, v IN weight2 | w + v) AS w2
        RETURN personIdsInPath, (w1 + w2) AS pathWeight
        ORDER BY pathWeight DESC
    