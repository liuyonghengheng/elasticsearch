GET _security/account

GET _security/role

GET _security/user/



PUT _security/account
{
    "current_password" : "pass1",
    "password" : "admin"
}


POST elasticsearch/_doc/1
{
  "public":true,
  "username":"admin",
  "security_attributes":"att1"
}

POST elasticsearch/_doc/2
{
  "public":false,
  "username":"medcl",
  "security_attributes":"att2"
}


POST elasticsearch/_doc/3
{
  "public":false,
  "username":"medcl",
  "security_attributes":"att3"
}


POST elasticsearch/_doc/4
{
  "public":false,
  "username":"medcl",
  "security_attributes":"att4"
}

GET elasticsearch/_search
{
  "query": {
    "bool": {
      "must": {
        "match": {
          "public": "true"
        }
      }
    }
  }
}

PUT _security/role/public_data
{
  "cluster": [
    "*"
  ],
  "indices": [{
    "names": [
      "elasticsearch"
    ],
    "query": "{\"term\": { \"public\": true}}",
    "privileges": [
      "read"
    ]
  }]
}


PUT _security/user/medcl
{
  "password": "pass",
  "roles": ["public_data"],
  "external_roles": ["captains", "starfleet"],
  "attributes": {
    "attribute1": "value1",
    "attribute2": "value2"
  }
}

GET _security/user/medcl

#-u medcl:pass
GET /elasticsearch/_search?pretty


#per user
PUT _security/role/public_data
{
  "cluster": [
    "*"
  ],
  "indices": [{
    "names": [
      "elasticsearch"
    ],
    "query": "{\"term\": { \"username\": \"${user.name}\"}}",
    "privileges": [
      "read"
    ]
  }]
}


PUT _security/role/abac
{
    "cluster": [
      "*"
    ],
  "indices": [{
    "names": [
      "*"
    ],
    "query": "{\"terms_set\": {\"security_attributes\": {\"terms\": [${attr.internal.permissions}], \"minimum_should_match_script\": {\"source\": \"doc['security_attributes.keyword'].length\"}}}}",
    "privileges": [
      "read"
    ]
  }]
}


PUT _security/user/user1
{
  "password": "pass",
  "roles": ["abac"],
  "attributes": {
    "permissions": "\"att1\", \"att2\", \"att3\""
  }
}


#field level security
POST movies/_doc/1
{
    "year": 2013,
    "title": "Rush",
    "actors": [
      "Daniel Brühl",
      "Chris Hemsworth",
      "Olivia Wilde"
    ]
}

POST movies/_doc/2
{
  "directors": [
    "Ron Howard"
  ],
  "plot": "A re-creation of the merciless 1970s rivalry between Formula One rivals James Hunt and Niki Lauda.",
  "genres": [
    "Action",
    "Biography",
    "Drama",
    "Sport"
  ]
}

PUT _security/role/limited_movie
{
  "cluster": [
    "*"
  ],
  "indices": [{
    "names": [
      "movies"
    ],
    "field_security":[
      "directors","year"
      ],
    "privileges": [
      "read"
    ]
  }]
}


PUT _security/user/medcl
{
  "password": "pass",
  "roles": ["limited_movie"]
}

#exclude genres
PUT _security/role/limited_movie
{
  "cluster": [
    "*"
  ],
  "indices": [
    {
      "names": [
        "movies"
      ],
      "field_security": [
        "~genres"
      ],
      "privileges": [
        "read"
      ]
    }
  ]
}


#masked
PUT _security/user/medcl
{
  "password": "pass",
  "roles": ["masked_movie"]
}

GET _security/privilege/

PUT _security/privilege/my
{
  "privileges": [
    "indices:data/write/index*",
    "indices:data/write/update*",
    "indices:admin/mapping/put",
    "indices:data/write/bulk*",
    "read",
    "write"
  ]
}


PUT _security/role/myrole
{
  "cluster": [
    "my",
    "indices_monitor"
  ],
  "indices": [{
    "names": [
      "movies*"
    ],
    "query": "",
    "field_security": [],
    "masked_fields": [],
    "privileges": [
      "my"
    ]
  }]
}
