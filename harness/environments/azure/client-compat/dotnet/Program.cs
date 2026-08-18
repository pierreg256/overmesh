using System.Reflection;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;
using Azure.Core;
using Azure.Core.Pipeline;
using Azure.Identity;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;

return await Runner.ExecuteAsync();

internal static class Runner
{
    private static readonly AsyncLocal<string?> CurrentRequestId = new();

    public static async Task<int> ExecuteAsync()
    {
        var resultPath = Required("OVERMESH_CLIENT_COMPAT_RESULT_PATH");
        var endpoint = Required("OVERMESH_CLIENT_COMPAT_ENDPOINT").TrimEnd('/');
        var container = Required("OVERMESH_CLIENT_COMPAT_CONTAINER");
        var prefix = Required("OVERMESH_CLIENT_COMPAT_PREFIX").Trim('/');
        var runId = Required("OVERMESH_CLIENT_COMPAT_RUN_ID");
        var commit = Required("OVERMESH_CLIENT_COMPAT_COMMIT");
        var projectVersion = Required("OVERMESH_CLIENT_COMPAT_PROJECT_VERSION");
        var managedIdentityClientId = Required("OVERMESH_CLIENT_COMPAT_MI_CLIENT_ID");
        var dotnetSdkVersion = Environment.GetEnvironmentVariable(
            "OVERMESH_CLIENT_COMPAT_DOTNET_SDK_VERSION"
        );

        var operations = new List<Dictionary<string, object?>>();
        var status = "passed";
        string? error = null;
        string? trace = null;

        var simpleBlob = $"{prefix}/simple.txt";
        var blockBlob = $"{prefix}/block.bin";
        var simpleBytes = Encoding.UTF8.GetBytes(
            $"client=azure-sdk-dotnet\nrun={runId}\nblob=simple\n"
        );
        var blockPartOne = Encoding.UTF8.GetBytes(
            $"client=azure-sdk-dotnet|run={runId}|block=1|"
        );
        var blockPartTwo = Encoding.UTF8.GetBytes(
            $"client=azure-sdk-dotnet|run={runId}|block=2|"
        );
        var blockBytes = blockPartOne.Concat(blockPartTwo).ToArray();

        var options = new BlobClientOptions();
        options.AddPolicy(new ClientRequestIdPolicy(), HttpPipelinePosition.PerCall);
        var credential = new ManagedIdentityCredential(clientId: managedIdentityClientId);
        var service = new BlobServiceClient(new Uri(endpoint), credential, options);
        var containerClient = service.GetBlobContainerClient(container);
        var simpleClient = containerClient.GetBlobClient(simpleBlob);
        var blockClient = containerClient.GetBlockBlobClient(blockBlob);

        var simpleDeleted = false;
        var blockDeleted = false;
        var cleanupErrors = new List<string>();

        try
        {
            var putRequestId = RequestId(runId, "put-blob");
            await WithRequestIdAsync(
                putRequestId,
                async () =>
                {
                    using var stream = new MemoryStream(simpleBytes, writable: false);
                    await simpleClient.UploadAsync(stream);
                }
            );
            AddOperation(
                operations,
                "put_blob",
                "passed",
                ("blob", simpleBlob),
                ("size_bytes", simpleBytes.Length),
                ("sha256", Sha256Hex(simpleBytes)),
                ("request_id", putRequestId),
                ("request_id_mode", "explicit-x-ms-client-request-id")
            );

            var blockIds = new[]
            {
                Convert.ToBase64String(Encoding.UTF8.GetBytes("block-0001")),
                Convert.ToBase64String(Encoding.UTF8.GetBytes("block-0002")),
            };

            await WithRequestIdAsync(
                RequestId(runId, "put-block-1"),
                async () =>
                {
                    using var stream = new MemoryStream(blockPartOne, writable: false);
                    await blockClient.StageBlockAsync(blockIds[0], stream);
                }
            );
            await WithRequestIdAsync(
                RequestId(runId, "put-block-2"),
                async () =>
                {
                    using var stream = new MemoryStream(blockPartTwo, writable: false);
                    await blockClient.StageBlockAsync(blockIds[1], stream);
                }
            );
            AddOperation(
                operations,
                "put_block",
                "passed",
                ("blob", blockBlob),
                ("block_count", 2),
                (
                    "decoded_block_id_length",
                    Convert.FromBase64String(blockIds[0]).Length
                ),
                ("request_id_mode", "explicit-x-ms-client-request-id")
            );

            var commitRequestId = RequestId(runId, "put-block-list");
            await WithRequestIdAsync(
                commitRequestId,
                () => blockClient.CommitBlockListAsync(blockIds)
            );
            AddOperation(
                operations,
                "put_block_list",
                "passed",
                ("blob", blockBlob),
                ("size_bytes", blockBytes.Length),
                ("sha256", Sha256Hex(blockBytes)),
                ("request_id", commitRequestId),
                ("request_id_mode", "explicit-x-ms-client-request-id")
            );

            var blockList = await blockClient.GetBlockListAsync(BlockListTypes.All);
            if (blockList.Value.CommittedBlocks.Count() != 2)
            {
                throw new InvalidOperationException(
                    "Committed block count did not match the staged block count."
                );
            }
            AddOperation(
                operations,
                "get_block_list",
                "passed",
                ("blob", blockBlob),
                ("committed_block_count", blockList.Value.CommittedBlocks.Count()),
                (
                    "uncommitted_block_count",
                    blockList.Value.UncommittedBlocks.Count()
                )
            );

            var simpleDownload = await simpleClient.DownloadContentAsync();
            var downloadedSimpleBytes = simpleDownload.Value.Content.ToArray();
            if (!downloadedSimpleBytes.SequenceEqual(simpleBytes))
            {
                throw new InvalidOperationException(
                    "Downloaded simple blob bytes did not match the uploaded payload."
                );
            }
            AddOperation(
                operations,
                "get_blob",
                "passed",
                ("blob", simpleBlob),
                ("sha256", Sha256Hex(downloadedSimpleBytes))
            );

            var blockDownload = await blockClient.DownloadContentAsync();
            var downloadedBlockBytes = blockDownload.Value.Content.ToArray();
            if (!downloadedBlockBytes.SequenceEqual(blockBytes))
            {
                throw new InvalidOperationException(
                    "Downloaded block blob bytes did not match the committed payload."
                );
            }
            AddOperation(
                operations,
                "get_blob_large",
                "passed",
                ("blob", blockBlob),
                ("sha256", Sha256Hex(downloadedBlockBytes))
            );

            var properties = await blockClient.GetPropertiesAsync();
            if (properties.Value.ContentLength != blockBytes.Length)
            {
                throw new InvalidOperationException(
                    $"Blob properties reported content length {properties.Value.ContentLength}, expected {blockBytes.Length}."
                );
            }
            AddOperation(
                operations,
                "head_blob",
                "passed",
                ("blob", blockBlob),
                ("content_length", properties.Value.ContentLength)
            );

            var listedNames = new List<string>();
            var pageCount = 0;
            await foreach (
                var page in containerClient.GetBlobsAsync(prefix: $"{prefix}/")
                    .AsPages(default, 1)
            )
            {
                pageCount++;
                listedNames.AddRange(page.Values.Select(item => item.Name));
            }
            if (!listedNames.Contains(simpleBlob) || !listedNames.Contains(blockBlob))
            {
                throw new InvalidOperationException(
                    "Prefix listing did not contain every expected canary blob."
                );
            }
            if (pageCount < 2)
            {
                throw new InvalidOperationException(
                    "Expected paged listing to span at least two pages."
                );
            }
            AddOperation(
                operations,
                "list_blobs",
                "passed",
                ("blob_count", listedNames.Count),
                ("page_count", pageCount),
                ("blobs", listedNames.OrderBy(name => name).ToArray())
            );

            await WithRequestIdAsync(
                RequestId(runId, "delete-simple"),
                () => simpleClient.DeleteIfExistsAsync()
            );
            simpleDeleted = true;
            AddOperation(
                operations,
                "delete_blob",
                "passed",
                ("blob", simpleBlob),
                ("request_id_mode", "explicit-x-ms-client-request-id")
            );

            await WithRequestIdAsync(
                RequestId(runId, "delete-block"),
                () => blockClient.DeleteIfExistsAsync()
            );
            blockDeleted = true;
            AddOperation(
                operations,
                "delete_blob_large",
                "passed",
                ("blob", blockBlob),
                ("request_id_mode", "explicit-x-ms-client-request-id")
            );
        }
        catch (Exception ex)
        {
            status = "failed";
            error = $"{ex.GetType().Name}: {ex.Message}";
            trace = ex.ToString();
        }
        finally
        {
            if (!simpleDeleted)
            {
                try
                {
                    await WithRequestIdAsync(
                        RequestId(runId, "cleanup-simple"),
                        () => simpleClient.DeleteIfExistsAsync()
                    );
                }
                catch (Exception ex)
                {
                    cleanupErrors.Add($"simple cleanup: {ex.GetType().Name}: {ex.Message}");
                }
            }

            if (!blockDeleted)
            {
                try
                {
                    await WithRequestIdAsync(
                        RequestId(runId, "cleanup-block"),
                        () => blockClient.DeleteIfExistsAsync()
                    );
                }
                catch (Exception ex)
                {
                    cleanupErrors.Add($"block cleanup: {ex.GetType().Name}: {ex.Message}");
                }
            }

            if (cleanupErrors.Count > 0 && status == "passed")
            {
                status = "failed";
                error = string.Join("; ", cleanupErrors);
            }

            var payload = new Dictionary<string, object?>
            {
                ["client"] = "azure-sdk-dotnet",
                ["result"] = status,
                ["endpoint"] = endpoint,
                ["container"] = container,
                ["prefix"] = prefix,
                ["timestamp_utc"] = UtcNow(),
                ["commit"] = commit,
                ["project_version"] = projectVersion,
                ["tool_versions"] = new Dictionary<string, object?>
                {
                    ["dotnet_sdk"] = dotnetSdkVersion,
                    ["dotnet_framework"] = RuntimeInformation.FrameworkDescription,
                    ["azure_identity"] = AssemblyVersion(typeof(ManagedIdentityCredential)),
                    ["azure_storage_blobs"] = AssemblyVersion(typeof(BlobServiceClient)),
                },
                ["operations"] = operations,
            };

            if (!string.IsNullOrEmpty(error))
            {
                payload["error"] = error;
            }

            if (!string.IsNullOrEmpty(trace))
            {
                payload["traceback"] = trace.Split(Environment.NewLine);
            }

            var json = JsonSerializer.Serialize(
                payload,
                new JsonSerializerOptions { WriteIndented = true }
            );
            await File.WriteAllTextAsync(resultPath, json);
        }

        return status == "passed" ? 0 : 1;
    }

    private static string Required(string name) =>
        Environment.GetEnvironmentVariable(name)
        ?? throw new InvalidOperationException($"{name} must be set.");

    private static string UtcNow() =>
        DateTimeOffset.UtcNow.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'");

    private static string RequestId(string runId, string suffix) =>
        $"dotnet-{runId}-{suffix}"[..Math.Min(128, $"dotnet-{runId}-{suffix}".Length)];

    private static string Sha256Hex(byte[] bytes) =>
        Convert.ToHexString(
                System.Security.Cryptography.SHA256.HashData(bytes)
            )
            .ToLowerInvariant();

    private static async Task WithRequestIdAsync(string requestId, Func<Task> operation)
    {
        var previous = CurrentRequestId.Value;
        CurrentRequestId.Value = requestId;
        try
        {
            await operation();
        }
        finally
        {
            CurrentRequestId.Value = previous;
        }
    }

    private static async Task<T> WithRequestIdAsync<T>(string requestId, Func<Task<T>> operation)
    {
        var previous = CurrentRequestId.Value;
        CurrentRequestId.Value = requestId;
        try
        {
            return await operation();
        }
        finally
        {
            CurrentRequestId.Value = previous;
        }
    }

    private static void AddOperation(
        ICollection<Dictionary<string, object?>> operations,
        string name,
        string result,
        params (string Key, object? Value)[] details
    )
    {
        var payload = new Dictionary<string, object?>
        {
            ["name"] = name,
            ["result"] = result,
            ["timestamp_utc"] = UtcNow(),
        };
        foreach (var (key, value) in details)
        {
            payload[key] = value;
        }
        operations.Add(payload);
    }

    private static string AssemblyVersion(Type type) =>
        type.Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()
            ?.InformationalVersion
        ?? type.Assembly.GetName().Version?.ToString()
        ?? "unknown";

    private sealed class ClientRequestIdPolicy : HttpPipelineSynchronousPolicy
    {
        public override void OnSendingRequest(HttpMessage message)
        {
            if (!string.IsNullOrEmpty(CurrentRequestId.Value))
            {
                message.Request.ClientRequestId = CurrentRequestId.Value;
            }
        }
    }
}
