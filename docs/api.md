# Pipeline Submit

# Job Creation

# Job status

## Transformation attributes
    - minIndegree
    - maxIndegree
    - minOutdegree
    - maxOutdegree
    - transformationType

## Node Attributes
    - type : source

## attributes
// here we have a node that will be used as part of the original graph
{
    "nodeType":SourceNode,
    "method":"stream",
    "options":{
        type:"Dropdown",
        options:["kafka","storm","kinesis"]
    }
}
-- Pipeline
    -- Multiple Jobs
