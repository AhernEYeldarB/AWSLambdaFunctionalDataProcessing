using System;
using System.IO;
using System.Collections;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Net.Http;
using Newtonsoft.Json;
using Amazon.Lambda.Core;
using Amazon.Lambda.APIGatewayEvents;
using Amazon;
using Amazon.S3;
using Amazon.S3.Model;
using System.Text.RegularExpressions;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace AWSPipe
{

    public class Function
    {
        private static readonly RegionEndpoint bucketRegion = RegionEndpoint.EUWest1;
        private static IAmazonS3 S3Client = new AmazonS3Client(bucketRegion);

        public async Task<APIGatewayProxyResponse> FunctionHandler(APIGatewayProxyRequest apigProxyEvent, ILambdaContext context)
        {
            int rowsProcessed = 0;
            Func<Row, object> mapFunction = value =>
            {
                var obj = new
                {
                    _id = value._id,
                    index = value.index,
                    guid = value.guid,
                    isActive = value.isActive,
                    balance = value.balance,
                    picture = value.picture,
                    age = value.age,
                    eyeColor = value.eyeColor,
                    name = value.name,
                    gender = value.gender,
                    company = value.company,
                    email = value.email,
                    phone = value.phone,
                    address = value.address,
                    about = value.about,
                    registered = value.registered,
                    latitude = value.latitude,
                    longitude = value.longitude,
                    tags = value.tags,
                    friends = value.friends,
                    greeting = value.greeting,
                    favoriteFruit = value.favoriteFruit
                };
                return obj;
            };

            Func<dynamic, bool> filterPredicate1 = value =>
            {
                return value.eyeColor == "green";
            };
            Func<dynamic, bool> filterPredicate2 = value =>
            {
                return value.age > 15;
            };

            Func<IEnumerable, IEnumerable> pipeline = Activities.pipelineMaker(
                Activities.mapMaker<Row, dynamic>(mapFunction),
                Activities.filterMaker(filterPredicate1),
                Activities.filterMaker(filterPredicate2)
            );


            // IEnumerable<Row> humans;
            try
            {
                GetObjectRequest request = new GetObjectRequest
                {
                    BucketName = "fyp-test-aws",
                    Key = "random-personal-info5.json"
                };
                using (GetObjectResponse response = await S3Client.GetObjectAsync(request))
                using (Stream inStream = response.ResponseStream)
                using (StreamReader reader = new StreamReader(inStream))
                using (JsonReader r = new JsonTextReader(reader))
                {
                    string title = response.Metadata["x-amz-meta-title"]; // Assume you have "title" as medata added to the object.
                    string contentType = response.Headers["Content-Type"];
                    Console.WriteLine("Object metadata, Title: {0}", title);
                    Console.WriteLine("Content type: {0}", contentType);
                    IEnumerable<Row> iterator = r.SelectTokensWithRegex<Row>(new Regex(@"^\[\d+\]$"));
                    foreach (var x in pipeline(iterator))
                    {
                        rowsProcessed++;
                        if (rowsProcessed % 4 == 1)
                        {
                            Console.WriteLine(x.ToString());
                        }
                    }
                }
            }
            catch (AmazonS3Exception e)
            {
                // If bucket or object does not exist
                Console.WriteLine("Error encountered ***. Message:'{0}' when reading object", e.Message);
            }
            catch (Exception e)
            {
                Console.WriteLine("Unknown encountered on server. Message:'{0}' when reading object", e.Message);
            }



            // using (JsonTextWriter wr = JsonReaderExtensions.InitJsonOutStream(OutStream))
            // {
            // wr.WriteStartArray();
            // foreach (var h in pipeline(humans))
            // {
            //     // wr.SerialiseJsonToStream<dynamic>(h);
            //     Console.WriteLine(h);
            // }
            // wr.WriteEndArray();
            // }

            var body = new Dictionary<string, string>
            {
                { "Rows Processed", rowsProcessed.ToString() },
            };

            return new APIGatewayProxyResponse
            {
                Body = JsonConvert.SerializeObject(body),
                StatusCode = 200,
                Headers = new Dictionary<string, string> { { "Content-Type", "application/json" } }
            };
        }
    }
}
