const clientId = "PLACEHOLDER_COGNITO_CLIENT_ID";
const clientSecret = "PLACEHOLDER_COGNITO_CLIENT_SECRET";
const username = "PLACEHOLDER_USERNAME";
const password = "PLACEHOLDER_PASSWORD";

fetch({
    url: "https://cognito-idp.us-east-1.amazonaws.com/",
    method: 'POST',
    header: {
        'X-Amz-Target': 'AWSCognitoIdentityProviderService.InitiateAuth',
        'Content-Type': 'application/x-amz-json-1.1'
    },
    body: {
        mode: 'raw',
        raw: JSON.stringify({
            "AuthParameters": {
                "USERNAME": username,
                "PASSWORD": password
            },
            "AuthFlow": "USER_PASSWORD_AUTH",
            "ClientId": clientId
        }),
        options: {
            raw: {
                language: 'json'
            }
        }
    }
}, function (error, response) {
    console.log(response.json());
    // pm.environment.set("cognitoAccessToken", response.json().AuthenticationResult.AccessToken);
    // pm.environment.set("cognitoIdToken", response.json().AuthenticationResult.IdToken);
}).then(r => {
    console.log(r.json())
});


var clientId = pm.environment.get("cognitoClientId");
var clientSecret = pm.environment.get("cognitoClientSecret");
var username = pm.environment.get("cognitoUserName");
var password = pm.environment.get("cognitoUserPassword");
pm.sendRequest({
    url: "https://cognito-idp.us-east-1.amazonaws.com/",
    method: 'POST',
    header: {
        'X-Amz-Target': 'AWSCognitoIdentityProviderService.InitiateAuth',
        'Content-Type': 'application/x-amz-json-1.1'
    },
    body: {
        mode: 'raw',
        raw: JSON.stringify({
            "AuthParameters": {
                "USERNAME": username,
                "PASSWORD": password
            },
            "AuthFlow": "USER_PASSWORD_AUTH",
            "ClientId": clientId
        }),
        options: {
            raw: {
                language: 'json'
            }
        }
    }
}, function (error, response) {
    console.log(response.json());
    pm.environment.set("cognitoAccessToken", response.json().AuthenticationResult.AccessToken);
    pm.environment.set("cognitoIdToken", response.json().AuthenticationResult.IdToken);
});
