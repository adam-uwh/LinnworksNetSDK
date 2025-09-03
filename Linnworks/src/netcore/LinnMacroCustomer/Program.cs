using System;

namespace LinnMacroCustomer
{
    public class Program
    {
        public static void Main(string[] args)
        {
            //Replace the following with your application details and installation token
            Guid applicationId = new Guid("9509e3bb-f2e2-4857-b526-196f240e037d");
            Guid secretKey = new Guid("e5e6cd0c-23bf-4028-939a-2ed7d37ec089");
            Guid token = new Guid("fc227e55-5c7c-7076-1794-a0350cebdff2");

            var macro = SetupMacro(applicationId, secretKey, token);

            // Example values - replace with your actual values or get them from user input/config
            //string locationId = "00000000-0000-0000-0000-000000000000"; // Example location ID
            //bool ignoreUnknownSKUs = true;
            string Source = "DIRECT";
            string subSource = "MultiVery";
            string notifyAcknowledge = "TRUE";
            string notifyOOS = "FALSE";
            string notifyBIS = "FALSE";
            string notifyShipped = "FALSE";
            string notifyCancelled = "FALSE";
            int tagValue = 6;
            string newFolder = "New";
            string oosFolder = "Out of Stock";
            string bisFolder = "Back in Stock";
            string SFTPServer = "sftp.jrslsecure.com/";
            int SFTPPort = 22;
            string SFTPUsername = "Ecommerce";
            string SFTPPassword = "1f3942Ns";
            string SFTPFolderRoot = "DSV Operations/Customers/1611-Very";
            string acknowledgeDirectory = "Notify-Acknowledged";
            string oosDirectory = "Notify-OOS";
            string bisDirectory = "Notify-BIS";
            string shippedDirectory = "Notify-Shipped";
            string cancelDirectory = "Notify-Cancelled";
            string filetype = "Direct"; //Direct or API
            string sortField = "GENERAL_INFO_ORDER_ID";
            string sortDirection = "ASC";
            int lookBackDays = 5;

            macro.Execute(Source, subSource, notifyAcknowledge, notifyOOS, notifyBIS, notifyShipped, notifyCancelled, tagValue, newFolder, oosFolder, bisFolder, SFTPServer, SFTPPort, SFTPUsername, SFTPPassword, SFTPFolderRoot, acknowledgeDirectory, oosDirectory, bisDirectory, shippedDirectory, cancelDirectory, filetype, sortField, sortDirection, lookBackDays);

            /*string sources ="DIRECT";
            string subSources = "MultiVery";
            string accountNumber = "9999";
            string SFTPServer = "sftp.jrslsecure.com/";
            int SFTPPort = 22;
            string SFTPUsername = "Ecommerce";
            string SFTPPassword = "1f3942Ns";
            string SFTPFolderPath = "DSV Operations/Customers/1611-Very/Notify-Acknowledged";
            string sortField = "";
            string sortDirection = "";
            int lastDays = 2;
            bool ignoreUnknownSKUs = true;
            string extendedPropertyName = "";
            bool addShippingCharge = false;
            string shippingChargeSku = "";*/

           // macro.Execute(sources, subSources, accountNumber, SFTPServer, SFTPPort, SFTPUsername, SFTPPassword, SFTPFolderPath, sortField, sortDirection, lastDays, ignoreUnknownSKUs, extendedPropertyName, addShippingCharge, shippingChargeSku);


            //macro.Execute(
            //orderIds,
            //true,  // removeFromCompanyName
            //true,  // removeFromCustomerName
            //true,  // removeFromAddressLines
            //true,  // removeFromTownAndRegion
            //true,   // removeFromCountry
            //true   // removeFromPostCode
            //);

            //var orderIds = new Guid[] { new Guid("c53e6d3c-be84-4285-9700-aabf7e82fdc0") };

            //macro.Execute(orderIds);

            Console.WriteLine("Order checked");
            Console.Read();
        }

        private static LinnworksAPI.BaseSession Authorize(Guid applicationId, Guid secretKey, Guid token)
        {
            var controller = new LinnworksAPI.AuthController(new LinnworksAPI.ApiContext("https://api.linnworks.net"));

            return controller.AuthorizeByApplication(new LinnworksAPI.AuthorizeByApplicationRequest
            {
                ApplicationId = applicationId,
                ApplicationSecret = secretKey,
                Token = token
            });
        }

        private static LinnworksMacro.LinnworksMacro SetupMacro(Guid applicationId, Guid secretKey, Guid token)
        {
            var auth = Authorize(applicationId, secretKey, token);

            var context = new LinnworksAPI.ApiContext(auth.Token, auth.Server);

            var url = new Api2Helper().GetUrl(context.ApiServer);

            var macro = new LinnworksMacro.LinnworksMacro()
            {
                Api = new LinnworksAPI.ApiObjectManager(context),
                Api2 = new LinnworksAPI2.LinnworksApi2(auth.Token, url),
                Logger = new LoggerProxy(),
            };

            return macro;
        }
    }
}

