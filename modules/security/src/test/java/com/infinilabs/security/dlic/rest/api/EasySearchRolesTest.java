package com.infinilabs.security.dlic.rest.api;

import com.infinilabs.security.test.helper.rest.RestHelper;
import org.junit.Test;

public class ElasticSearchRolesTest extends AbstractRestApiUnitTest {

    // RoleMappingHolder.map是匹配角色， RestApiPrivilegesEvaluator.checkRoleBasedAccessPermissions 是校验，
    // security 内部都是按role来判断用户的权限
    // 1 取当前访问用户的角色是固定2种方式，1是 按用户名匹配角色 user1 [test1]
    // 2 是 按当前用户的backend_roles 匹配角色  admin [superuser] external_roles
    // 3 按 and_external_roles 来判断，这块还没细看，官方没找到使用的例子
    // 4 用户要使用REST API，需要具备 security.restapi.roles_enabled 里指定的角色，在checkRoleBasedAccessPermissions里校验， 目前默认是在elasticsearch.yml里指定的2个 security.restapi.roles_enabled: [ "superuser", "security_rest_api_access" ]
    // 所以我们使用时如果想让用户能访问 restapi 得单独的给用户绑定security_rest_api_access角色， 目前看只能在创建用户时绑定，rolemapping能否指定还要再多试下

    @Test
    public void testROleMappingBackendsRole() throws Exception {
        setup();

        rh.keystore = "restapi/kirk-keystore.jks";
        rh.sendAdminCertificate = false;

//        RestHelper.HttpResponse response = rh
//            .executeGetRequest("_security/" + CType.USER.toLCString());
//        Assert.assertEquals(HttpStatus.SC_OK, response.getStatusCode());


//        RestHelper.HttpResponse response = rh.executeGetRequest("/_security/role_mapping", encodeBasicHeader("admin", "admin"));
//        System.out.println("role_mapping::: "+response.getBody());

        RestHelper.HttpResponse response = rh.executePutRequest("/_security/role/test1",
            "{\"cluster\":[\"all\"],\"indices\":[{\"names\":[\"index1\",\"index2\"],\"privileges\":[\"all\"],\"field_security\":[],\"query\":\"{\\\"match\\\": {\\\"title\\\": \\\"foo\\\"}}\"}]}",
            encodeBasicHeader("admin", "admin"));

//        System.out.println("create role test1::: "+response.getBody());

        response = rh.executePutRequest("/_security/user/user1",
            "{\"external_roles\":[\"role2\"],\"attributes\":{},\"password\":\"user1\"}", encodeBasicHeader("admin", "admin"));
        System.out.println("create user user1::: "+response.getBody());


        response = rh.executePutRequest("/_security/role_mapping/test1",
            "{\"external_roles\":[\"admin\"],\"users\":[\"user1\"]}",
            encodeBasicHeader("admin", "admin"));
        System.out.println("create role_mapping test1::: "+response.getBody());


        response = rh.executeGetRequest("/_security/role_mapping",
            encodeBasicHeader("admin", "admin"));
        System.out.println("get role_mapping::: "+response.getBody());

        response = rh.executeGetRequest("/_security/user", encodeBasicHeader("admin", "admin"));
        System.out.println("admin::: "+response.getBody());

        response = rh.executeGetRequest("/_security/user/user1", encodeBasicHeader("user1", "user1"));
        System.out.println("user1::: "+response.getBody());

    }
}
