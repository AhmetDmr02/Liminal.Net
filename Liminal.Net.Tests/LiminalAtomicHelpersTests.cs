using Liminal.Net.Misc;
using NUnit.Framework;
using System;

namespace Liminal.Net.Tests
{
    [TestFixture]
    public class LiminalAtomicHelpersTests
    {
        [Test]
        public void SafeInvoke_MiddleDelegateThrows_DoesNotReinvokeEarlierOrFailedDelegates()
        {
            int firstCount = 0;
            int secondCount = 0;
            int thirdCount = 0;

            void Step1() => firstCount++;
            void Step2() { secondCount++; throw new InvalidOperationException("Step2 failed"); }
            void Step3() => thirdCount++;

            Action multi = null;
            multi += Step1;
            multi += Step2;
            multi += Step3;

            LiminalAtomicHelpers.SafeInvoke(multi);

            Assert.That(firstCount, Is.EqualTo(1), "Step1 was re-invoked or missed!");
            Assert.That(secondCount, Is.EqualTo(1), "Step2 was re-invoked after throwing!");
            Assert.That(thirdCount, Is.EqualTo(1), "Step3 was not executed!");
        }

        [Test]
        public void SafeInvoke_LastDelegateThrows_DoesNotReinvokeEarlierDelegates()
        {
            int firstCount = 0;
            int secondCount = 0;
            int thirdCount = 0;

            void Step1() => firstCount++;
            void Step2() => secondCount++;
            void Step3() { thirdCount++; throw new InvalidOperationException("Step3 failed"); }

            Action multi = null;
            multi += Step1;
            multi += Step2;
            multi += Step3;

            LiminalAtomicHelpers.SafeInvoke(multi);

            Assert.That(firstCount, Is.EqualTo(1), "Step1 was re-invoked or missed!");
            Assert.That(secondCount, Is.EqualTo(1), "Step2 was re-invoked!");
            Assert.That(thirdCount, Is.EqualTo(1), "Step3 was re-invoked after throwing!");
        }

        [Test]
        public void SafeInvoke_WithArg_MiddleDelegateThrows_DoesNotReinvokeEarlierDelegates()
        {
            int firstCount = 0;
            int secondCount = 0;
            int thirdCount = 0;

            void Step1(int val) => firstCount += val;
            void Step2(int val) { secondCount += val; throw new InvalidOperationException("Step2 failed"); }
            void Step3(int val) => thirdCount += val;

            Action<int> multi = null;
            multi += Step1;
            multi += Step2;
            multi += Step3;

            LiminalAtomicHelpers.SafeInvoke(multi, 5);

            Assert.That(firstCount, Is.EqualTo(5), "Step1 was re-invoked or missed!");
            Assert.That(secondCount, Is.EqualTo(5), "Step2 was re-invoked!");
            Assert.That(thirdCount, Is.EqualTo(5), "Step3 was not executed!");
        }

        [Test]
        public void SafeInvoke_WithThreeArgs_MiddleDelegateThrows_DoesNotReinvokeEarlierDelegates()
        {
            int firstCount = 0;
            int secondCount = 0;
            int thirdCount = 0;

            void Step1(int a, string b, bool c) => firstCount += a;
            void Step2(int a, string b, bool c) { secondCount += a; throw new InvalidOperationException("Step2 failed"); }
            void Step3(int a, string b, bool c) => thirdCount += a;

            Action<int, string, bool> multi = null;
            multi += Step1;
            multi += Step2;
            multi += Step3;

            LiminalAtomicHelpers.SafeInvoke(multi, 10, "test", true);

            Assert.That(firstCount, Is.EqualTo(10), "Step1 was re-invoked or missed!");
            Assert.That(secondCount, Is.EqualTo(10), "Step2 was re-invoked!");
            Assert.That(thirdCount, Is.EqualTo(10), "Step3 was not executed!");
        }

        [Test]
        public void SafeInvoke_FirstDelegateThrows_InvokesRemainingDelegatesOnce()
        {
            int firstCount = 0;
            int secondCount = 0;
            int thirdCount = 0;

            void Step1() { firstCount++; throw new InvalidOperationException("Step1 failed"); }
            void Step2() => secondCount++;
            void Step3() => thirdCount++;

            Action multi = null;
            multi += Step1;
            multi += Step2;
            multi += Step3;

            LiminalAtomicHelpers.SafeInvoke(multi);

            Assert.That(firstCount, Is.EqualTo(1), "Step1 was re-invoked after throwing!");
            Assert.That(secondCount, Is.EqualTo(1), "Step2 was not executed!");
            Assert.That(thirdCount, Is.EqualTo(1), "Step3 was not executed!");
        }

        [Test]
        public void SafeInvoke_HappyPath_ExecutesAllExactlyOnce()
        {
            int firstCount = 0;
            int secondCount = 0;

            void Step1() => firstCount++;
            void Step2() => secondCount++;

            Action multi = null;
            multi += Step1;
            multi += Step2;

            LiminalAtomicHelpers.SafeInvoke(multi);

            Assert.That(firstCount, Is.EqualTo(1));
            Assert.That(secondCount, Is.EqualTo(1));
        }

        [Test]
        public void SafeInvoke_SingleDelegateThrows_DoesNotCrash()
        {
            int count = 0;
            Action single = () => { count++; throw new InvalidOperationException("Single failed"); };

            Assert.DoesNotThrow(() => LiminalAtomicHelpers.SafeInvoke(single));
            Assert.That(count, Is.EqualTo(1));
        }
    }
}
