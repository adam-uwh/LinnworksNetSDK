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
            /// Configure Parameters for UW Home Linnworks Default Channel Updater Macro 
            /// //////////////////////////////////////////////////////////////////////////

            /// Parameters
            string Source = "DATAIMPORTEXPORT";
            string subSource = "MultiVery";
            string notifyAcknowledge = "TRUE";
            string notifyOOS = "TRUE";
            string notifyBIS = "TRUE";
            string notifyShipped = "TRUE";
            string actionCancelled = "TRUE";
            string notifyCancelled = "TRUE";
            string newFolder = "NEW";
            string oosFolder = "Out of Stock";
            string bisFolder = "Back in Stock";
            string cancelFolder = "To Be Cancelled";
            string SFTPServer = "sftp.jrslsecure.com";
            int SFTPPort = 22;
            string SFTPUsername = "Ecommerce";
            string SFTPPassword = "1f3942Ns";
            string SFTPFolderRoot = "DSV Operations/Customers/1611-Very";
            string acknowledgeDirectory = "Notify-Acknowledged";
            string oosDirectory = "Notify-OOS";
            string bisDirectory = "Notify-BIS";
            string shippedDirectory = "Notify-Shipped";
            string actionCancelledDirectory = "Notify-Cancelled";
            string cancelDirectory = "Notify-Cancelled";
            string filetype = "Direct";
            string sortField = "GENERAL_INFO_ORDER_ID";
            string sortDirection = "ASC";
            int lookBackDays = 5;
            string localFilePath = @"C:\Users\adamw\OneDrive - johnhogggroup\Documents\Projects\LinnworksMacro";
            string outputMethod = "Local"; /// Local or FTP;

            macro.Execute(
            Source,
            subSource,
            notifyAcknowledge,
            notifyOOS,
            notifyBIS,
            notifyShipped,
            actionCancelled,
            notifyCancelled,
            newFolder,
            oosFolder,
            bisFolder,
            cancelFolder,
            SFTPServer,
            SFTPPort,
            SFTPUsername,
            SFTPPassword,
            SFTPFolderRoot,
            acknowledgeDirectory,
            oosDirectory,
            bisDirectory,
            shippedDirectory,
            actionCancelledDirectory,
            cancelDirectory,
            filetype,
            sortField,
            sortDirection,
            lookBackDays,
            localFilePath,
            outputMethod
            );


            //////////////////////////////////////////////////////////////////////////////
            /// END Configure Parameters for UW Home Linnworks Default Channel Updater Macro  
            /// //////////////////////////////////////////////////////////////////////////

            //////////////////////////////////////////////////////////////////////////////
            /// Configure Parameters for UW Home Linnworks Default Stock Check Macro 
            /// //////////////////////////////////////////////////////////////////////////
            
            // Order IDs to process (replace with actual order GUIDs for testing)
            /*Guid[] orderIds = new Guid[]
            {
                new Guid("7d1ce80b-6b5d-4e39-9346-2ecc9b528c15"), // Replace with actual order ID
                // Add more order IDs as needed
            };*/

            /*
            // Folder names
            string locationId = "00000000-0000-0000-0000-000000000000";
            string checkFolder = "Out of Stock";
            string outOfStockFolder = "Out of Stock";
            string toBeCancelledFolder = "To Be Cancelled";
            string newFolder = "Back in Stock";
            string updatedFolder = "Updated";
            bool ignoreUnknownSKUs = true;

            // Extended property names
            string channelUpdatesRequiredProperty = "ChannelUpdatesRequired";
            string backOrdersProperty = "BackOrders";

            // Execute the macro with all parameters
            macro.Execute(
                locationId,
                checkFolder,
                outOfStockFolder,
                toBeCancelledFolder,
                newFolder,
                updatedFolder,
                channelUpdatesRequiredProperty,
                backOrdersProperty,
                ignoreUnknownSKUs
            );

            */
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

