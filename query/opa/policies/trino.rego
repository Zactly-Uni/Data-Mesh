package trino.authz

default allow = true

# admin_user und api_user dürfen alles
allow if {
    input.context.identity.user == "admin_user"
}

allow if {
    input.context.identity.user == "api_user"
}

allow if {
    input.context.identity.user == "metabase"
}

# autobahn_user darf nur auf Autobahndaten zugreifen
allow if {
    input.context.identity.user == "autobahn_user"
    input.resource.schema == ""
}
