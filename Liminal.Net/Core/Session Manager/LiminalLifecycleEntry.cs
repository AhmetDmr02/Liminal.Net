namespace Liminal.Net.Core
{
    internal enum LiminalLifecycleKind : byte
    {
        //Only ends the session, raises nothing
        SessionClosed = 0,
        ClientConnected,
        ClientDisconnected,
        ClientKicked,
        LocalClientConnected,
        LocalClientDisconnected,
        DisconnectResolved,
        Starvation
    }

    //Transport threads queue these, the tick thread raises them in order with the packets around them
    internal readonly struct LiminalLifecycleEntry
    {
        public readonly LiminalLifecycleKind Kind;
        public readonly ushort ClientId;
        public readonly LiminalSession Session;
        public readonly DisconnectReason Reason;
        public readonly string Message;
        public readonly ServerStarvationEvent Starvation;

        public LiminalLifecycleEntry(LiminalLifecycleKind kind, ushort clientId, LiminalSession session)
        {
            Kind = kind;
            ClientId = clientId;
            Session = session;
            Reason = DisconnectReason.Unknown;
            Message = null;
            Starvation = default;
        }

        private LiminalLifecycleEntry(ushort clientId, DisconnectReason reason, string message, ServerStarvationEvent starvation, LiminalLifecycleKind kind)
        {
            Kind = kind;
            ClientId = clientId;
            Session = null;
            Reason = reason;
            Message = message;
            Starvation = starvation;
        }

        public static LiminalLifecycleEntry Resolved(ushort clientId, DisconnectReason reason, string message) =>
            new LiminalLifecycleEntry(clientId, reason, message, default, LiminalLifecycleKind.DisconnectResolved);

        public static LiminalLifecycleEntry Starved(ServerStarvationEvent starvation) =>
            new LiminalLifecycleEntry(starvation.ClientId, DisconnectReason.Unknown, null, starvation, LiminalLifecycleKind.Starvation);
    }
}
