namespace TelemetryService
{
    public static class Retrier
    {
        public static async Task Do(Action action, TimeSpan retryInterval, int retryCount = 3)
        {
            var exceptions = new List<Exception>();

            for (int retry = 0; retry < retryCount; retry++)
            {
                try
                {
                    action();
                    return;
                }
                catch (Exception ex)
                {
                    exceptions.Add(ex);
                    await Task.Delay(retryInterval);
                }
            }
            throw exceptions.Last();
        }

        public static async Task DoAsync(Task action, TimeSpan retryInterval, int retryCount = 3)
        {
            var exceptions = new List<Exception>();

            for (int retry = 0; retry < retryCount; retry++)
            {
                try
                {
                    await action;
                    return;
                }
                catch (OperationCanceledException)
                {
                    return;
                }
                catch (Exception ex)
                {
                    exceptions.Add(ex);
                    await Task.Delay(retryInterval);
                }
            }
            throw exceptions.Last();
        }
    }
}
