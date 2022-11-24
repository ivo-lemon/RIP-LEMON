const axios = require('axios');
const NodeRSA = require('node-rsa');
const crypto = require('crypto');
   
// This public and private key are the same as in the files and were generated using https://cryptotools.net/rsagen 
// They are RSA 4096 bit and with this we can corroborate fireblocks webhook code in: https://docs.fireblocks.com/api/?javascript#webhook
// So with this we can mock that we hit Kotlin! And also use it for integration tests
const publicKey = `-----BEGIN PUBLIC KEY-----
MIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEAgxqrQysjqkFHJlxB27v1
we4UIM+/wO/SKQ43WG4b4utkA8dChZI2IIzTcFYXhYre/KfFAl87+pvAonZhlfzA
mC0gJw9RuZyhMmQz3DHxHIU3uK51WjSSgJ0Vvhouy/yDYh3XzhGXRpcbnDx2voaA
vKL/FJdCSDvuhZKx/xlr5QkisO7Xi2g31vjSeMmnqiMH0j+hlXxMJcXf7xcDFcuW
UnA/4FmrEbN7tWILrOfuEw9sNlRSP116Wzj9JTy5MaSWpqaPNQ+nUjngon5AS7Ea
4RaKhoYLePLW127uyexHzDJV9qhkzB8fiJ9Lpn8F1yE14FccCpjzlUv/AYXwpdVq
GZswojQYICXcqMB4ZXBxZ1cf74Soe0WSO0bbpLP8pm4UhA7TvDAuLe9AnZk1oeE2
4STQQBhjtGhUNzrfefBtVyZpYkKsjy5OXxAmoWnPBOjJ6bXI0VzgUVHRhkUxCs/3
X3MtZCyhVSTUGR1i5Ebo4jmYKJnHYco/pxNyJ9Yf3XyjayxHt9rUVYvvLkK/luOh
snQ5CW5xkjrqYn+r6izR84C90sS3D/x8fW9H22mFs/Tkjloobrska0KF0V/5YMfJ
pVQ2zBLnIyfFHsFH13WXoBBapR7vb2GpCSLUXjYExXEiObTl+s57bBoPtKmhi72j
8PEhpIZSIioX+AVWdQMxGqECAwEAAQ==
-----END PUBLIC KEY-----`.replace(/\\n/g, "\n");

const privateKey = `-----BEGIN RSA PRIVATE KEY-----
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
-----END RSA PRIVATE KEY-----`.replace(/\\n/g, "\n");

const http = require('http');
const port = process.env.PORT || 3000;

const server = http.createServer((req, res) => {
  res.statusCode = 200;
  const msg = 'Hello Node!\n'
  res.end(msg);
});

server.listen(port, () => {
  console.log(`Server running on http://localhost:${port}/`);
  const data = '{"not_type":"TRANSACTION_STATUS_UPDATED","tenantId":"42b6fa26-d38b-5b8f-b58e-ffbd364deced","timestamp":1645131241220,"data":{"id":"093a9742-8f3b-4848-b392-d82bc289b9ac","createdAt":1645131230413,"lastUpdated":1645131230935,"assetId":"MATIC_BSC","source":{"id":"9","type":"VAULT_ACCOUNT","name":"Cash-Out","subType":""},"destination":{"id":null,"type":"ONE_TIME_ADDRESS","name":"N/A","subType":""},"amount":35,"sourceAddress":"","destinationAddress":"addr_test1qzd6jeczsgu22nnk9m4fmpnj6x7d7s9gpz97eqa933qyxfmphkusy4x9msrc00j2p7trutyfhec9e3wslh7z2nzrjd7qlpdvyq","destinationAddressDescription":"","destinationTag":"e91ab819-bee8-44a6-b4db-6fc78eee5782","status":"FAILED","txHash":"","subStatus":"INSUFFICIENT_FUNDS","signedBy":[],"createdBy":"05787fb0-488d-e18c-4683-45a689ee6b84","rejectedBy":"","amountUSD":59.15,"addressType":"","note":"65f93e19-d56b-4d10-9818-b6995ddf57d8","exchangeTxId":"","requestedAmount":35,"feeCurrency":"MATIC_BSC","operation":"TRANSFER","customerRefId":"e91ab819-bee8-44a6-b4db-6fc78eee5782","amountInfo":{"amount":"35","requestedAmount":"35","amountUSD":"59.15"},"feeInfo":{},"destinations":[],"externalTxId":"65f93e19-d56b-4d10-9818-b6995ddf57d8","blockInfo":{},"signedMessages":[]}}'
  //const data = "some test string"
  //const data = '{"type":"TRANSACTION_STATUS_UPDATED","tenantId":"123","timestamp":123123123,"data":{"id":"1fb5748d-a5ba-4ddd-a134-8e3341979471","assetId":"ETH","status":"REJECTED","txHash":"0x42a5f4998f2af435eb7f00967e061568906d5b0520a5a789cc886316eab6eb11","networkFee":0.001,"signedBy":[],"rejectedBy":"000332e7-d3f6-4697-855d-88b19eead8a6","feeCurrency":"ETH","externalTxId":"INVALID_LEMON_EXTERNAL_TRANSACTION_ID","numOfConfirmations":0}}'
  /*const data = JSON.stringify({
    "type":"TRANSACTION_CREATED",
    "tenantId":"42b6fa26-d38b-5b8f-b58e-ffbd364deced",
    "timestamp":1644505122320,
    "data":{
      "id":"62d32b5d-5d49-4e8d-9e8e-8a670e973a5d",
      "createdAt":1644504508860,
      "lastUpdated":1644504508860,
      "assetId":"ETH_TEST",
      "source":{
        "id":"1",
        "type":"VAULT_ACCOUNT",
        "name":"Test account",
        "subType":""
      },
      "destination":{
        "id":"9",
        "type":"VAULT_ACCOUNT",
        "name":"Cash-Out",
        "subType":""
      },
      "amount":0.05,
      "sourceAddress":"",
      "destinationAddress":"",
      "destinationAddressDescription":"",
      "destinationTag":"",
      "status":"SUBMITTED",
      "txHash":"",
      "subStatus":"",
      "signedBy":[],
      "createdBy":"554e4d71-a5a8-d6c5-a616-ec997bd2f32d",
      "rejectedBy":"",
      "amountUSD":155.33,
      "addressType":"",
      "note":"Doesthiswork?",
      "exchangeTxId":"",
      "requestedAmount":0.05,
      "feeCurrency":"ETH_TEST",
      "operation":"TRANSFER",
      "customerRefId":null,
      "amountInfo":{
        "amount":"0.05",
        "requestedAmount":"0.05",
        "amountUSD":"155.33"
      },
      "feeInfo":{},
      "destinations":[],
      "externalTxId":null,
      "blockInfo":{},
      "signedMessages":[]
    }
  })*/
  //const data = "some test data for my input stream";
  console.log(data)

  const sign = crypto.createSign('RSA-SHA512');
  sign.write(data);
  sign.end();

  const base64Signature = sign.sign(privateKey, "base64");
  console.log(base64Signature)
  console.log('-------')

  //const signature = req.headers("Fireblocks-Signature");
  const verifier = crypto.createVerify('RSA-SHA512');
  verifier.write(data);
  verifier.end();

  const isVerified = verifier.verify(publicKey, base64Signature, "base64");
  console.log("Verified:", isVerified);
});


