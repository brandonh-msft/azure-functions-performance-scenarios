using CSharpPrecompiledDFScale;
using DurableTask.Core;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace V2FunctionsLoadTest
{
    public static class Runner
    {
        [FunctionName(nameof(StartDFScaleTest))]
        public static async Task<IActionResult> StartDFScaleTest(
#pragma warning disable IDE0060 // Remove unused parameter
            [HttpTrigger(AuthorizationLevel.Function, methods: "post", Route = nameof(StartDFScaleTest) + @"/{instances=600}/{parallelism=100}")] HttpRequest req,
#pragma warning restore IDE0060 // Remove unused parameter
            [OrchestrationClient] DurableOrchestrationClientBase client,
            int instances, int parallelism,
            ILogger log)
        {
            var currentInstances = (await client.GetStatusAsync())
                .Where(i => !i.RuntimeStatus.IsFinished())
                .ToList();
            if (currentInstances.Any())
            {
                log.LogInformation($@"We had {currentInstances.Count} instances still hanging around. Clearing them out...");

                foreach (var id in currentInstances)
                {
                    await client.TerminateAsync(id.InstanceId, @"New test run started. Purging.");
                }

                log.LogInformation(@"Done.");
            }

            // purge everything
            await client.PurgeInstanceHistoryAsync(DateTime.MinValue, null, Enum.GetValues(typeof(OrchestrationStatus)).Cast<OrchestrationStatus>());

            log.LogTrace($@"Queuing {instances} instances...");
            var tasks = new List<Task<string>>();
            for (var i = 0; i < instances; i++)
            {
                tasks.Add(client.StartNewAsync(nameof(StartWorkflow), parallelism));
            }

            StateTracker.InstanceIds = new List<string>(await Task.WhenAll(tasks));
            log.LogTrace(@"They're all started!");

            return new AcceptedObjectResult(new { NumInstancesProcessing = StateTracker.InstanceIds.Count });
        }

        class AcceptedObjectResult : ObjectResult
        {
            private readonly string _location;

            public AcceptedObjectResult(object value) : this(nameof(GetStatus), value) { }
            public AcceptedObjectResult(string location, object value) : base(value)
            {
                _location = location;
            }

            public override Task ExecuteResultAsync(ActionContext context)
            {
                context.HttpContext.Response.StatusCode = 202;
                var uri = new UriBuilder(context.HttpContext.Request.Scheme, context.HttpContext.Request.Host.Host)
                {
                    Path = $@"api/{_location}",
                };
                if (context.HttpContext.Request.Host.Port.HasValue)
                {
                    uri.Port = context.HttpContext.Request.Host.Port.Value;
                }

                context.HttpContext.Response.Headers.Add(@"Location", uri.ToString());

                return base.ExecuteResultAsync(context);
            }
        }

        [FunctionName(nameof(GetStatus))]
        public static async Task<IActionResult> GetStatus(
#pragma warning disable IDE0060 // Remove unused parameter
            [HttpTrigger(AuthorizationLevel.Function, methods: "get")] HttpRequest req,
#pragma warning restore IDE0060 // Remove unused parameter
            [OrchestrationClient] DurableOrchestrationClientBase client,
            ILogger log)
        {
            if (StateTracker.InstanceIds?.Any() == true)
            {
                var instancesWithStatus = StateTracker.InstanceIds
                    .Select(async i => (Id: i, Status: await client.GetStatusAsync(i)))
                    .ToList();  // avoid re-running the LINQ statement w/ every iteration

                log.LogTrace($@"Checking {instancesWithStatus.Count} for any completions...");
                foreach (var i in instancesWithStatus)
                {
                    var result = await i;
                    if (result.Status.RuntimeStatus == OrchestrationRuntimeStatus.Completed)
                    {
                        log.LogTrace($@"Id ({result.Id}) showing 'Completed' status. Removing from state.");
                        StateTracker.InstanceIds.Remove(result.Id);
                    }
                    else
                    {
                        log.LogTrace($@"{result.Id}: {result.Status.RuntimeStatus}");
                    }
                }

                log.LogTrace($@"{StateTracker.InstanceIds.Count} left!");

                if (StateTracker.InstanceIds.Any())
                {
                    return new AcceptedObjectResult(new { NumInstancesProcessing = StateTracker.InstanceIds.Count });
                }

            }

            return new OkResult();
        }
    }

    static class Extensions
    {
        public static bool IsFinished(this OrchestrationRuntimeStatus status) =>
            status == OrchestrationRuntimeStatus.Canceled
            || status == OrchestrationRuntimeStatus.Completed
            || status == OrchestrationRuntimeStatus.Failed
            || status == OrchestrationRuntimeStatus.Terminated;
    }
}
