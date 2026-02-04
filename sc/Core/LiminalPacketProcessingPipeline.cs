using Liminal.Net.Interfaces;

namespace Liminal.Net.Core
{
    public class LiminalPacketProcessingPipeline
    {
        private List<ILiminalPacketProcessor> _packetProcessorList;
        public LiminalPacketProcessingPipeline(List<ILiminalPacketProcessor> packetProcessorList)
        {
            _packetProcessorList = packetProcessorList;
        }
    }
}
