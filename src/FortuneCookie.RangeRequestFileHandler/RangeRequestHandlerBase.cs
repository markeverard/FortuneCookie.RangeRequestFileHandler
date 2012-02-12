using System;
using System.Collections.Specialized;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Web;

namespace FortuneCookie.RangeRequestFileHandler
{
    /// <summary>
    /// An abstract HTTP Handler that provides resumable file downloads in ASP.NET.
    /// 
    /// Created by:
    ///     Scott Mitchell
    ///     mitchell@4guysfromrolla.com
    ///     http://dotnetslackers.com/articles/aspnet/Range-Specific-Requests-in-ASP-NET.aspx
    /// </summary>
    public abstract class RangeRequestHandlerBase : IHttpHandler
    {
        #region Constants
        private const string MULTIPART_BOUNDARY= "<q1w2e3r4t5y6u7i8o9p0>";
        private const string MULTIPART_CONTENTTYPE= "multipart/byteranges; boundary=" + MULTIPART_BOUNDARY;
        private const string DEFAULT_CONTENTTYPE = "application/octet-stream";
        private const string HTTP_HEADER_ACCEPT_RANGES= "Accept-Ranges";
        private const string HTTP_HEADER_ACCEPT_RANGES_BYTES = "bytes";
        private const string HTTP_HEADER_ACCEPT_RANGES_NONE = "none";
        private const string HTTP_HEADER_CONTENT_TYPE= "Content-Type";
        private const string HTTP_HEADER_CONTENT_RANGE= "Content-Range";
        private const string HTTP_HEADER_CONTENT_LENGTH= "Content-Length";
        private const string HTTP_HEADER_ENTITY_TAG= "ETag";
        private const string HTTP_HEADER_LAST_MODIFIED= "Last-Modified";
        private const string HTTP_HEADER_RANGE= "Range";
        private const string HTTP_HEADER_IF_RANGE= "If-Range";
        private const string HTTP_HEADER_IF_MATCH= "If-Match";
        private const string HTTP_HEADER_IF_NONE_MATCH= "If-None-Match";
        private const string HTTP_HEADER_IF_MODIFIED_SINCE= "If-Modified-Since";
        private const string HTTP_HEADER_IF_UNMODIFIED_SINCE= "If-Unmodified-Since";
        private const string HTTP_HEADER_UNLESS_MODIFIED_SINCE= "Unless-Modified-Since";
        private const string HTTP_METHOD_GET= "GET";
        private const string HTTP_METHOD_HEAD= "HEAD";
    
        private const int DEBUGGING_SLEEP_TIME = 0;
        #endregion
      
        protected RangeRequestHandlerBase()
        {
            ProcessRequestCheckSteps = new Func<HttpContext, bool>[]
                    {
                        CheckAuthorizationRules,
                        CheckHttpMethod,
                        CheckFileRequested,
                        CheckRangesRequested,
                        CheckIfModifiedSinceHeader,
                        CheckIfUnmodifiedSinceHeader,
                        CheckIfMatchHeader,
                        CheckIfNoneMatchHeader,
                        CheckIfRangeHeader
                    };
        }

        #region Properties
        /// <summary>
        /// Indicates if the HTTP request is for multiple ranges.
        /// </summary>
        public bool IsMultipartRequest { get; private set; }

        /// <summary>
        /// Indicates if the HTTP request is for one or more ranges.
        /// </summary>
        public bool IsRangeRequest { get; private set; }

        /// <summary>
        /// The start byte(s) for the requested range(s).
        /// </summary>
        public long[] StartRangeBytes { get; private set; }

        /// <summary>
        /// The end byte(s) for the requested range(s).
        /// </summary>
        public long[] EndRangeBytes { get; private set; }

        /// <summary>
        /// The size of each chunk of data streamed back to the client.
        /// </summary>
        /// <remarks>
        /// When a client makes a range request the requested file's contents are
        /// read in BufferSize chunks, with each chunk flushed to the output stream
        /// until the requested byte range has been read.
        /// </remarks>
        public virtual int BufferSize { get { return 10240; } }

        /// <summary>
        /// Indicates whether Range requests are enabled. If false, the HTTP Handler
        /// ignores the Range HTTP Header and returns the entire contents.
        /// </summary>
        public virtual bool EnableRangeRequests { get { return true; } }

        public bool IsReusable { get { return false; } }

        private Func<HttpContext, bool>[] ProcessRequestCheckSteps { get; set; }
        private FileInfo InternalRequestedFileInfo { get; set; }
        private string InternalRequestedFileEntityTag { get; set; }
        private string InternalRequestedFileMimeType { get; set; }
        private readonly NameValueCollection _internalResponseHeaders = new NameValueCollection();
        #endregion

        /// <summary>
        /// Returns a FileInfo object representing the requested content.
        /// </summary>
        public abstract FileInfo GetRequestedFileInfo(HttpContext context);
    
        /// <summary>
        /// Returns the Entity Tag (ETag) for the requested content.
        /// </summary>
        /// <remarks>
        /// The Entity Tag is computed by taking the physical path to the file, concatenating it with the
        /// file's created date and time, and computing the MD5 hash of that string.
        /// 
        /// A derived class MAY override this method to return an Entity Tag
        /// value computed using an alternate approach.
        /// </remarks>
        public virtual string GetRequestedFileEntityTag(HttpContext context)
        {
            FileInfo requestedFile = GetRequestedFileInfo(context);
            if (requestedFile == null)
                return string.Empty;

            ASCIIEncoding ascii = new ASCIIEncoding();
            byte[] sourceBytes = ascii.GetBytes(
                string.Concat(
                    requestedFile.FullName,
                    "|",
                    requestedFile.LastWriteTimeUtc
                    )
                );

            return Convert.ToBase64String(new MD5CryptoServiceProvider().ComputeHash(sourceBytes));
        }
    
        /// <summary>
        /// Returns the MIME type for the requested content.
        /// </summary>
        /// <remarks>
        /// A dervied class SHOULD override this method and return the MIME type specific
        /// to the requested content. See http://www.iana.org/assignments/media-types/ for
        /// a list of MIME types registered with the Internet Assigned Numbers Authority (IANA).
        /// </remarks>
        public virtual string GetRequestedFileMimeType(HttpContext context)
        {
            return DEFAULT_CONTENTTYPE;
        }

        public void ProcessRequest(HttpContext context)
        {
            InternalRequestedFileInfo = GetRequestedFileInfo(context);
            InternalRequestedFileEntityTag = GetRequestedFileEntityTag(context);
            InternalRequestedFileMimeType = GetRequestedFileMimeType(context);
            ParseRequestHeaderRanges(context);

            // Perform each check; exit if any check returns false
            if (ProcessRequestCheckSteps.Any(check => check(context) == false))
            {
               return;
            }

            if (!EnableRangeRequests || !IsRangeRequest)
                ReturnEntireEntity(context);
            else
                ReturnPartialEntity(context);
        }

        private void ReturnEntireEntity(HttpContext context)
        {
            HttpRequest request = context.Request;
            HttpResponse response = context.Response;

            response.StatusCode = 200;  // OK
            WriteCommonResponseHeaders(response, InternalRequestedFileInfo.Length, InternalRequestedFileMimeType);
        

            if (request.HttpMethod.Equals(HTTP_METHOD_HEAD) == false)
                response.TransmitFile(InternalRequestedFileInfo.FullName);
        }

        private void ReturnPartialEntity(HttpContext context)
        {
            HttpRequest request = context.Request;
            HttpResponse response = context.Response;

            response.StatusCode = 206;  // Partial response
        
            // Specify the byte range being returned for non-multipart requests
            if (IsMultipartRequest == false)
                AddHeader(response, HTTP_HEADER_CONTENT_RANGE,
                          string.Format("bytes {0}-{1}/{2}", StartRangeBytes[0], EndRangeBytes[0], InternalRequestedFileInfo.Length));

            WriteCommonResponseHeaders(response, 
                                       ComputeContentLength(),
                                       IsMultipartRequest ? MULTIPART_CONTENTTYPE : InternalRequestedFileMimeType);

            if (request.HttpMethod.Equals(HTTP_METHOD_HEAD) == false)
                ReturnChunkedResponse(context);
        }

        private void ReturnChunkedResponse(HttpContext context)
        {
            HttpResponse response = context.Response;

            byte[] buffer = new byte[BufferSize];
            using (FileStream fs = InternalRequestedFileInfo.OpenRead())
            {
                for (int i = 0; i < StartRangeBytes.Length; i++)
                {
                    // Position the stream at the starting byte
                    fs.Seek(StartRangeBytes[i], SeekOrigin.Begin);

                    int bytesToReadRemaining = Convert.ToInt32(EndRangeBytes[i] - StartRangeBytes[i]) + 1;

                    // Output multipart boundary, if needed
                    if (IsMultipartRequest)
                    {
                        response.Output.Write("--" + MULTIPART_BOUNDARY);
                        response.Output.Write(string.Format("{0}: {1}", HTTP_HEADER_CONTENT_TYPE, InternalRequestedFileMimeType));
                        response.Output.Write(string.Format("{0}: bytes {1}-{2}/{3}",
                                                            HTTP_HEADER_CONTENT_RANGE,
                                                            StartRangeBytes[i],
                                                            EndRangeBytes[i],
                                                            InternalRequestedFileInfo.Length));
                        response.Output.WriteLine();
                    }

                    // Stream out the requested chunks for the current range request
                    while (bytesToReadRemaining > 0)
                    {
                        if (!response.IsClientConnected)
                            return;

                        int chunkSize = fs.Read(buffer, 0,
                                                BufferSize < bytesToReadRemaining
                                                    ? BufferSize
                                                    : bytesToReadRemaining);
                        response.OutputStream.Write(buffer, 0, chunkSize);

                        bytesToReadRemaining -= chunkSize;
                        response.Flush();
                        System.Threading.Thread.Sleep(DEBUGGING_SLEEP_TIME);
                    }
                }

                fs.Close();
            }
        }

        private int ComputeContentLength()
        {
            int contentLength = 0;

            for (int i = 0; i < StartRangeBytes.Length; i++)
            {
                contentLength += Convert.ToInt32(EndRangeBytes[i] - StartRangeBytes[i]) + 1;

                if (IsMultipartRequest)
                    contentLength += MULTIPART_BOUNDARY.Length
                                     + InternalRequestedFileMimeType.Length
                                     + StartRangeBytes[i].ToString().Length
                                     + EndRangeBytes[i].ToString().Length
                                     + InternalRequestedFileInfo.Length.ToString().Length
                                     + 49;       // Length needed for multipart header
            }

            if (IsMultipartRequest)
                contentLength += MULTIPART_BOUNDARY.Length + 8;    // Length of dash and line break

            return contentLength;
        }

        private void WriteCommonResponseHeaders(HttpResponse Response, long contentLength, string contentType)
        {
            AddHeader(Response, HTTP_HEADER_CONTENT_LENGTH, contentLength.ToString());
            AddHeader(Response, HTTP_HEADER_CONTENT_TYPE, contentType);
            AddHeader(Response, HTTP_HEADER_LAST_MODIFIED, InternalRequestedFileInfo.LastWriteTimeUtc.ToString("r"));
            AddHeader(Response, HTTP_HEADER_ENTITY_TAG, string.Concat("\"", InternalRequestedFileEntityTag, "\""));

            if (EnableRangeRequests)
                AddHeader(Response, HTTP_HEADER_ACCEPT_RANGES, HTTP_HEADER_ACCEPT_RANGES_BYTES);
            else
                AddHeader(Response, HTTP_HEADER_ACCEPT_RANGES, HTTP_HEADER_ACCEPT_RANGES_NONE);
        }

        private string RetrieveHeader(HttpRequest Request, string headerName, string defaultValue)
        {
            return string.IsNullOrEmpty(Request.Headers[headerName]) ? defaultValue : Request.Headers[headerName].Replace("\"", string.Empty);
        }

        protected virtual void ParseRequestHeaderRanges(HttpContext context)
        {
            HttpRequest request = context.Request;
           
            string rangeHeader = RetrieveHeader(request, HTTP_HEADER_RANGE, string.Empty);

            if (string.IsNullOrEmpty(rangeHeader))
            {
                // No Range HTTP Header supplied; send back entire contents
                StartRangeBytes = new long[] { 0 };
                EndRangeBytes = new long[] { InternalRequestedFileInfo.Length - 1 };
                IsRangeRequest = false;
                IsMultipartRequest = false;
            }
            else
            {
                // rangeHeader contains the value of the Range HTTP Header and can have values like:
                //      Range: bytes=0-1            * Get bytes 0 and 1, inclusive
                //      Range: bytes=0-500          * Get bytes 0 to 500 (the first 501 bytes), inclusive
                //      Range: bytes=400-1000       * Get bytes 500 to 1000 (501 bytes in total), inclusive
                //      Range: bytes=-200           * Get the last 200 bytes
                //      Range: bytes=500-           * Get all bytes from byte 500 to the end
                //
                // Can also have multiple ranges delimited by commas, as in:
                //      Range: bytes=0-500,600-1000 * Get bytes 0-500 (the first 501 bytes), inclusive plus bytes 600-1000 (401 bytes) inclusive

                // Remove "Ranges" and break up the ranges
                string[] ranges = rangeHeader.Replace("bytes=", string.Empty).Split(",".ToCharArray());

                StartRangeBytes = new long[ranges.Length];
                EndRangeBytes = new long[ranges.Length];

                IsRangeRequest = true;
                IsMultipartRequest = (StartRangeBytes.Length > 1);

                for (int i = 0; i < ranges.Length; i++)
                {
                    const int START = 0, END = 1;

                    // Get the START and END values for the current range
                    string[] currentRange = ranges[i].Split("-".ToCharArray());

                    if (string.IsNullOrEmpty(currentRange[END]))
                        // No end specified
                        EndRangeBytes[i] = InternalRequestedFileInfo.Length - 1;
                    else
                        // An end was specified
                        EndRangeBytes[i] = long.Parse(currentRange[END]);

                    if (string.IsNullOrEmpty(currentRange[START]))
                    {
                        // No beginning specified, get last n bytes of file
                        StartRangeBytes[i] = InternalRequestedFileInfo.Length - 1 - EndRangeBytes[i];
                        EndRangeBytes[i] = InternalRequestedFileInfo.Length - 1;
                    }
                    else
                    {
                        // A normal begin value
                        StartRangeBytes[i] = long.Parse(currentRange[0]);
                    }
                }
            }
        }

        /// <summary>
        /// Adds an HTTP Response Header
        /// </summary>
        /// <remarks>
        /// This method is used to store the Response Headers in a private, member variable,
        /// InternalResponseHeaders, so that the Response Headers may be accesed in the
        /// LogResponseHttpHeaders method, if needed. The Response.Headers property can only
        /// be accessed directly when using IIS 7's Integrated Pipeline mode. This workaround
        /// permits logging of Response Headers when using Classic mode or a web server other
        /// than IIS 7.
        /// </remarks>
        protected void AddHeader(HttpResponse response, string name, string value)
        {
            _internalResponseHeaders.Add(name, value);
            response.AddHeader(name, value);
        }

        #region Process Request Step Checks
        protected virtual bool CheckAuthorizationRules(HttpContext context)
        {
            return true;
        }

        protected virtual bool CheckHttpMethod(HttpContext context)
        {
            HttpRequest request = context.Request;
            HttpResponse response = context.Response;

            if (!request.HttpMethod.Equals(HTTP_METHOD_GET) &&
                !request.HttpMethod.Equals(HTTP_METHOD_HEAD))
            {
                response.StatusCode = 501;  // Not Implemented
                return false;
            }

            return true;
        }

        protected virtual bool CheckFileRequested(HttpContext context)
        {
            HttpResponse response = context.Response;

            if (InternalRequestedFileInfo == null)
            {
                response.StatusCode = 404;  // Not Found
                return false;
            }

            if (InternalRequestedFileInfo.Length > int.MaxValue)
            {
                response.StatusCode = 413; // Request Entity Too Large
                return false;
            }

            return true;
        }

        protected virtual bool CheckRangesRequested(HttpContext context)
        {
            for (int i = 0; i < StartRangeBytes.Length; i++)
            {
                if (StartRangeBytes[i] > InternalRequestedFileInfo.Length - 1 ||
                    EndRangeBytes[i] > InternalRequestedFileInfo.Length - 1)
                {
                    context.Response.StatusCode = 400; // Bad Request
                    return false;
                }

                if (StartRangeBytes[i] < 0 || EndRangeBytes[i] < 0)
                {
                    context.Response.StatusCode = 400; // Bad Request
                    return false;
                }

                if (EndRangeBytes[i] >= StartRangeBytes[i]) 
                    continue;

                context.Response.StatusCode = 400; // Bad Request
                return false;
            }
            return true;
        }

        protected virtual bool CheckIfModifiedSinceHeader(HttpContext context)
        {
            HttpRequest request = context.Request;
            HttpResponse response = context.Response;

            string ifModifiedSinceHeader = RetrieveHeader(request, HTTP_HEADER_IF_MODIFIED_SINCE, string.Empty);

            if (!string.IsNullOrEmpty(ifModifiedSinceHeader))
            {
                // Determine the date
                DateTime ifModifiedSinceDate;
                DateTime.TryParse(ifModifiedSinceHeader, out ifModifiedSinceDate);

                if (ifModifiedSinceDate == DateTime.MinValue)
                    // Could not parse date... do not continue on with check
                    return true;

                DateTime requestedFileModifiedDate = InternalRequestedFileInfo.LastWriteTimeUtc;
                requestedFileModifiedDate = new DateTime(
                    requestedFileModifiedDate.Year,
                    requestedFileModifiedDate.Month,
                    requestedFileModifiedDate.Day,
                    requestedFileModifiedDate.Hour,
                    requestedFileModifiedDate.Minute,
                    requestedFileModifiedDate.Second
                    );
                ifModifiedSinceDate = ifModifiedSinceDate.ToUniversalTime();

                if (requestedFileModifiedDate <= ifModifiedSinceDate)
                {
                    // File was created before specified date
                    response.StatusCode = 304;  // Not Modified
                    return false;
                }
            }

            return true;
        }

        protected virtual bool CheckIfUnmodifiedSinceHeader(HttpContext context)
        {
            HttpRequest request = context.Request;
            HttpResponse response = context.Response;

            string ifUnmodifiedSinceHeader = RetrieveHeader(request, HTTP_HEADER_IF_UNMODIFIED_SINCE, string.Empty);

            if (string.IsNullOrEmpty(ifUnmodifiedSinceHeader))
                // Look for Unless-Modified-Since header
                ifUnmodifiedSinceHeader = RetrieveHeader(request, HTTP_HEADER_UNLESS_MODIFIED_SINCE, string.Empty);

            if (!string.IsNullOrEmpty(ifUnmodifiedSinceHeader))
            {
                // Determine the date
                DateTime ifUnmodifiedSinceDate;
                DateTime.TryParse(ifUnmodifiedSinceHeader, out ifUnmodifiedSinceDate);

                DateTime requestedFileModifiedDate = InternalRequestedFileInfo.LastWriteTimeUtc;
                requestedFileModifiedDate = new DateTime(
                    requestedFileModifiedDate.Year,
                    requestedFileModifiedDate.Month,
                    requestedFileModifiedDate.Day,
                    requestedFileModifiedDate.Hour,
                    requestedFileModifiedDate.Minute,
                    requestedFileModifiedDate.Second
                    );
                if (ifUnmodifiedSinceDate != DateTime.MinValue)
                    ifUnmodifiedSinceDate = ifUnmodifiedSinceDate.ToUniversalTime();

                if (requestedFileModifiedDate > ifUnmodifiedSinceDate)
                {
                    // Could not convert value into date or file was created after specified date
                    response.StatusCode = 412;  // Precondition failed
                    return false;
                }
            }

            return true;
        }

        protected virtual bool CheckIfMatchHeader(HttpContext context)
        {
            HttpRequest request = context.Request;
            HttpResponse response = context.Response;

            string ifMatchHeader = RetrieveHeader(request, HTTP_HEADER_IF_MATCH, string.Empty);

            if (string.IsNullOrEmpty(ifMatchHeader) || ifMatchHeader == "*")
                return true;        // Match found

            // Look for a matching ETag value in ifMatchHeader
            string[] entityIds = ifMatchHeader.Replace("bytes=", string.Empty).Split(",".ToCharArray());

            if (entityIds.Any(entityId => InternalRequestedFileEntityTag == entityId))
                return true; // Match found

            // If we reach here, no match found
            response.StatusCode = 412;  // Precondition failed
            return false;
        }

        protected virtual bool CheckIfNoneMatchHeader(HttpContext context)
        {
            HttpRequest request = context.Request;
            HttpResponse response = context.Response;

            string ifNoneMatchHeader = RetrieveHeader(request, HTTP_HEADER_IF_NONE_MATCH, string.Empty);

            if (string.IsNullOrEmpty(ifNoneMatchHeader))
                return true;

            if (ifNoneMatchHeader == "*")
            {
                // Logically invalid request
                response.StatusCode = 412;  // Precondition failed
                return false;
            }

            // Look for a matching ETag value in ifNoneMatchHeader
            string[] entityIds = ifNoneMatchHeader.Replace("bytes=", string.Empty).Split(",".ToCharArray());

            foreach (string entityId in entityIds.Where(entityId => InternalRequestedFileEntityTag == entityId))
            {
                AddHeader(response, HTTP_HEADER_ENTITY_TAG, string.Concat("\"", entityId, "\""));
                response.StatusCode = 304;  // Not modified
                return false;        // Match found
            }

            // No match found
            return true;
        }

        protected virtual bool CheckIfRangeHeader(HttpContext context)
        {
            HttpRequest request = context.Request;
           
            string ifRangeHeader = RetrieveHeader(request, HTTP_HEADER_IF_RANGE, InternalRequestedFileEntityTag);

            if (IsRangeRequest && ifRangeHeader != InternalRequestedFileEntityTag)
            {
                ReturnEntireEntity(context);
                return false;
            }
            return true;
        }
        #endregion
    }
}