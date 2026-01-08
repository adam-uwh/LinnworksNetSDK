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

            //////////////////////////////////////////////////////////////////////////////
            /// Configure Parameters for UW Home Linnworks Default Stock Check Macro 
            /// //////////////////////////////////////////////////////////////////////////
            
            // Order IDs to process (replace with actual order GUIDs for testing)
            Guid[] orderIds = new Guid[]
            {
                new Guid("00000000-0000-0000-0000-000000000000"), // Replace with actual order ID
                // Add more order IDs as needed
            };

            // Folder names
            string outOfStockFolder = "Out of Stock";
            string toBeCancelledFolder = "To Be Cancelled";
            string newFolder = "New";
            string updatedFolder = "Updated";

            // Extended property names
            string channelUpdatesRequiredProperty = "ChannelUpdatesRequired";
            string backOrdersProperty = "BackOrders";

            // Execute the macro with all parameters
            macro.Execute(
                orderIds,
                outOfStockFolder,
                toBeCancelledFolder,
                newFolder,
                updatedFolder,
                channelUpdatesRequiredProperty,
                backOrdersProperty
            );

            //////////////////////////////////////////////////////////////////////////////
            /// END Configure Parameters for UW Home Linnworks Default Stock Check Macro 
            /// //////////////////////////////////////////////////////////////////////////


            //////////////////////////////////////////////////////////////////////////////
            /// Configure Parameters for UW Home B2B JDE Export Macro 
            /// //////////////////////////////////////////////////////////////////////////
            // Set required variables
            /*
            string source                = "";
            string subSource             = "UW Home";
            string accountNumber         = "";

            string SFTPServer            = "sftp.jrslsecure.com/";
            int    SFTPPort              = 22;
            string SFTPUsername          = "Ecommerce";
            string SFTPPassword          = "1f3942Ns";
            string SFTPFolderPath        = "DSV Operations/Customers/";

            string localFilePath         = @"C:\Users\adamw\OneDrive - johnhogggroup\Documents\Projects\LinnworksMacro";

            string sortField             = "";
            string sortDirection         = "";
            int    lastDays              = 20;

            bool   addShippingCharge     = true;
            string shippingChargeSku     = "NS100000000ZZ";
            string extendedPropertyName  = "soldto";

            string priceFlag             = "P";
            string orderType             = "SO";
            string branchPlan            = "90";
            string shipTo                = "";
            string shipCode              = "";
            string holdStatus            = "";

            bool   addDispatchDays       = true;  // corrected type
            int    dispatchModifier      = 11;

            string folderUpdated         = "Updated";
            string folderCompleted       = "In JDE";

            bool   ignoreUnknownSKUs     = true;


            // Pass variables to macro.Execute (match signature in LinnworksMacro)
            macro.Execute(
                source,
                subSource,
                accountNumber,
                SFTPServer,
                SFTPPort,
                SFTPUsername,
                SFTPPassword,
                SFTPFolderPath,
                localFilePath,
                sortField,
                sortDirection,
                lastDays,
                addShippingCharge,
                shippingChargeSku,
                extendedPropertyName,
                priceFlag,
                orderType,
                branchPlan,
                shipTo,
                shipCode,
                holdStatus,
                addDispatchDays,
                dispatchModifier,
                folderUpdated,
                folderCompleted,
                ignoreUnknownSKUs
            );
            */
            //////////////////////////////////////////////////////////////////////////////
            /// END Configure Parameters for UW Home B2B JDE Export Macro 
            /// //////////////////////////////////////////////////////////////////////////

            Console.WriteLine("Processed order check complete.");
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

