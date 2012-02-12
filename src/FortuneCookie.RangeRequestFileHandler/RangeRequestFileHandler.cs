using System.IO;
using System.Web;
using EPiServer.Web;
using EPiServer.Web.Hosting;

namespace FortuneCookie.RangeRequestFileHandler
{
    public class RangeRequestFileHandler : RangeRequestHandlerBase
    {
        /// <summary>
        /// Returns a FileInfo object from the mapped VirtualPathProviderFile
        /// </summary>
        /// <param name="context"></param>
        /// <returns></returns>
        public override FileInfo GetRequestedFileInfo(HttpContext context)
        {
            UnifiedFile file = GetFileInfoFromVirtualPathProvider(context);

            if (file == null)
                return null;

            PreventRewriteOnOutgoingStream();
            return new FileInfo(file.LocalPath);
        }

        private static UnifiedFile GetFileInfoFromVirtualPathProvider(HttpContext context)
        {
            return GenericHostingEnvironment.VirtualPathProvider.GetFile(context.Request.FilePath) as UnifiedFile;
        }

        /// <summary>
        /// Returns the MIME type from the mapped VirtualPathProviderFile
        /// </summary>
        /// <param name="context"></param>
        /// <returns></returns>
        public override string GetRequestedFileMimeType(HttpContext context)
        {
            UnifiedFile file = GetFileInfoFromVirtualPathProvider(context);
            return MimeMapping.GetMimeMapping(file.Name);
        }

        /// <summary>
        /// Prevents episerver rewrite on outgoing stream.
        /// </summary>
        private void PreventRewriteOnOutgoingStream()
        {
            if (UrlRewriteProvider.Module != null)
                UrlRewriteProvider.Module.FURLRewriteResponse = false;
        }
    }
}
