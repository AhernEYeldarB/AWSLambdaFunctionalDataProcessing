using System.IO;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using System.Runtime.CompilerServices;

using Newtonsoft.Json;

[assembly: InternalsVisibleTo("../Function")]

namespace AWSPipe
{
    public static class JsonReaderExtensions
    {
        // Returns an iterable that yields JSON objects from the structure given in the regex. In this case [ { obj1 }, { obj2 } ]
        public static IEnumerable<T> SelectTokensWithRegex<T>(
            this Newtonsoft.Json.JsonReader jsonReader, Regex regex)
        {
            JsonSerializer serializer = new JsonSerializer();
            while (jsonReader.Read())
            {
                if (regex.IsMatch(jsonReader.Path)
                    && jsonReader.TokenType != JsonToken.PropertyName)
                {
                    yield return serializer.Deserialize<T>(jsonReader);
                }
            }
        }

        // https://stackoverflow.com/questions/18269958/writing-json-to-a-stream-without-buffering-the-string-in-memory
        public static void SerialiseJsonToStream<T>(
            this JsonTextWriter jsonWriter, object value)
        {
            JsonSerializer serializer = new JsonSerializer();
            serializer.Serialize(jsonWriter,
                                 value);
        }

        public static JsonTextWriter InitJsonOutStream(Stream s)
        {
            // Create JSON generator
            StreamWriter sw = new StreamWriter(s);
            JsonTextWriter wr = new JsonTextWriter(sw);

            return wr;
        }
        public static IEnumerable<Row> convertToJsonIterable(Stream s)
        {
            // Create JSON generator
            var regex = new Regex(@"^\[\d+\]$");
            IEnumerable<Row> iterableRows;
            StreamReader sr = new StreamReader(s);
            JsonReader reader = new JsonTextReader(sr);
            iterableRows = reader.SelectTokensWithRegex<Row>(regex);
            return iterableRows;
        }
        public static IEnumerable<Pipe> convertToJsonIterable(string s)
        {
            // Create JSON generator
            var regex = new Regex(@"^\[\d+\]$");
            IEnumerable<Pipe> iterableJson;
            StringReader sr = new StringReader(s);
            JsonReader reader = new JsonTextReader(sr);
            iterableJson = reader.SelectTokensWithRegex<Pipe>(regex);
            return iterableJson;
        }
    }
}