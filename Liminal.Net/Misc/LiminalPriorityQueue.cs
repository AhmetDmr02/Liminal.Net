using System;
using System.Collections.Generic;

namespace Liminal.Net.Core
{
    public sealed class LiminalPriorityQueue<TElement, TPriority>
    {
        private readonly List<(TElement Element, TPriority Priority)> _nodes = new();
        private readonly IComparer<TPriority> _comparer;

        public int Count => _nodes.Count;

        public LiminalPriorityQueue() : this(Comparer<TPriority>.Default) { }

        public LiminalPriorityQueue(IComparer<TPriority> comparer)
        {
            _comparer = comparer ?? Comparer<TPriority>.Default;
        }

        public void Enqueue(TElement element, TPriority priority)
        {
            _nodes.Add((element, priority));
            int childIndex = _nodes.Count - 1;

            while (childIndex > 0)
            {
                int parentIndex = (childIndex - 1) / 2;
                if (_comparer.Compare(_nodes[childIndex].Priority, _nodes[parentIndex].Priority) >= 0)
                    break;

                Swap(childIndex, parentIndex);
                childIndex = parentIndex;
            }
        }

        public TElement Dequeue()
        {
            if (_nodes.Count == 0)
                throw new InvalidOperationException("Queue is empty.");

            TElement root = _nodes[0].Element;
            int lastIndex = _nodes.Count - 1;
            _nodes[0] = _nodes[lastIndex];
            _nodes.RemoveAt(lastIndex);

            int parentIndex = 0;
            while (true)
            {
                int firstChild = (parentIndex * 2) + 1;
                if (firstChild >= _nodes.Count)
                    break;

                int secondChild = firstChild + 1;
                int bestChild = (secondChild < _nodes.Count &&
                                 _comparer.Compare(_nodes[secondChild].Priority, _nodes[firstChild].Priority) < 0)
                    ? secondChild
                    : firstChild;

                if (_comparer.Compare(_nodes[bestChild].Priority, _nodes[parentIndex].Priority) >= 0)
                    break;

                Swap(parentIndex, bestChild);
                parentIndex = bestChild;
            }

            return root;
        }

        public bool TryPeek(out TElement element, out TPriority priority)
        {
            if (_nodes.Count == 0)
            {
                element = default;
                priority = default;
                return false;
            }

            element = _nodes[0].Element;
            priority = _nodes[0].Priority;
            return true;
        }

        public void Clear() => _nodes.Clear();

        private void Swap(int a, int b)
        {
            var temp = _nodes[a];
            _nodes[a] = _nodes[b];
            _nodes[b] = temp;
        }
    }
}