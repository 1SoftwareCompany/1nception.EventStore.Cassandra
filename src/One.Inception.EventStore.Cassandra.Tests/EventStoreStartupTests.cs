using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using One.Inception.AtomicAction;
using One.Inception.Multitenancy;

namespace One.Inception.EventStore.Cassandra.Tests;

public class EventStoreStartupTests
{
    [Fact]
    public void It_runs_once_for_the_whole_cluster()
    {
        Assert.True(StartupScanner.IsClusterWide(typeof(EventStoreStartup)));
    }

    [Fact]
    public async Task Tenants_which_are_bootstrapped_in_parallel_on_a_node_create_their_storage_one_after_another()
    {
        var @lock = new InMemoryLock();
        var startup = new RecordingEventStoreStartup(@lock, storageDuration: TimeSpan.FromMilliseconds(20));
        string[] tenants = Enumerable.Range(1, 10).Select(i => $"tenant{i}").ToArray();

        await Task.WhenAll(tenants.Select(tenant => startup.BootstrapAsync([tenant])));

        Assert.Equal(tenants.OrderBy(t => t), startup.Created.OrderBy(t => t));
        Assert.Equal(1, startup.MaxConcurrent);
        Assert.Equal(0, @lock.Refused);
    }

    [Fact]
    public async Task It_waits_for_another_node_which_creates_the_storage_of_another_tenant()
    {
        var @lock = new InMemoryLock { RefuseTimes = 3 };
        var startup = new RecordingEventStoreStartup(@lock);

        await startup.BootstrapAsync(["tenant1"]);

        Assert.Equal(["tenant1"], startup.Created);
    }

    [Fact]
    public async Task It_creates_the_storage_without_the_lock_when_it_cannot_take_it()
    {
        var @lock = new InMemoryLock { RefuseTimes = int.MaxValue };
        var startup = new RecordingEventStoreStartup(@lock);

        await startup.BootstrapAsync(["tenant1"]);

        Assert.Equal(["tenant1"], startup.Created);
        Assert.Equal(EventStoreStartup.MaxAttempts, @lock.Refused);
    }

    [Fact]
    public async Task It_releases_the_locks_when_the_storage_cannot_be_created()
    {
        var @lock = new InMemoryLock();
        var startup = new RecordingEventStoreStartup(@lock) { FailFor = "tenant1" };

        await Assert.ThrowsAsync<TimeoutException>(() => startup.BootstrapAsync(["tenant1"]));
        await startup.BootstrapAsync(["tenant2"]);

        Assert.Equal(["tenant2"], startup.Created);
        Assert.Equal(0, @lock.Refused);
    }

    class RecordingEventStoreStartup : EventStoreStartup
    {
        private readonly TimeSpan storageDuration;
        private int running;
        private int maxConcurrent;

        public RecordingEventStoreStartup(ILock @lock, TimeSpan storageDuration = default)
            : base(new Monitor<BoundedContext>(new BoundedContext { Name = "tests" }), null, new Monitor<TenantsOptions>(new TenantsOptions()), @lock, NullLogger<EventStoreStartup>.Instance, _ => TimeSpan.Zero)
        {
            this.storageDuration = storageDuration;
        }

        public string FailFor { get; init; }

        public List<string> Created { get; } = new List<string>();

        public int MaxConcurrent => maxConcurrent;

        internal override async Task CreateStorageAsync(string tenant)
        {
            int now = Interlocked.Increment(ref running);
            InterlockedMax(ref maxConcurrent, now);
            try
            {
                await Task.Delay(storageDuration);
                if (tenant == FailFor)
                    throw new TimeoutException("Cassandra did not answer");

                lock (Created) Created.Add(tenant);
            }
            finally
            {
                Interlocked.Decrement(ref running);
            }
        }

        private static void InterlockedMax(ref int target, int value)
        {
            int current;
            while (value > (current = Volatile.Read(ref target)) && Interlocked.CompareExchange(ref target, value, current) != current) { }
        }
    }

    /// <summary>
    /// A lock which is refused while it is held, and the first <see cref="RefuseTimes"/> times as if another node holds it.
    /// </summary>
    class InMemoryLock : ILock
    {
        private readonly HashSet<string> held = new HashSet<string>();
        private int refused;

        public int RefuseTimes { get; set; }

        public int Refused => refused;

        public Task<bool> IsLockedAsync(string resource) { lock (held) return Task.FromResult(held.Contains(resource)); }

        public Task<bool> LockAsync(string resource, TimeSpan ttl)
        {
            lock (held)
            {
                if (RefuseTimes > 0 || held.Add(resource) == false)
                {
                    if (RefuseTimes > 0 && RefuseTimes != int.MaxValue)
                        RefuseTimes--;
                    refused++;
                    return Task.FromResult(false);
                }
                return Task.FromResult(true);
            }
        }

        public Task UnlockAsync(string resource) { lock (held) held.Remove(resource); return Task.CompletedTask; }
    }

    class Monitor<T> : IOptionsMonitor<T>
    {
        public Monitor(T value) => CurrentValue = value;

        public T CurrentValue { get; }

        public T Get(string name) => CurrentValue;

        public IDisposable OnChange(Action<T, string> listener) => null;
    }
}
