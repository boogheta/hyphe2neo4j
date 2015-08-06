MATCH (we:WebEntity {status: "IN"})-[:PREFIX]->(:Stem)-[rels:HERIT*0..]->(n:Stem)
WHERE ALL(n IN extract(rel in rels | endNode(rel)) WHERE NOT (n)<-[:PREFIX]-())
                     
MATCH (n)-[:LINK]-()<-[:HERIT*0..]-()<-[:PREFIX]-(twe:WebEntity {status: "IN"})
WHERE twe <> we      
RETURN DISTINCT we, twe;

