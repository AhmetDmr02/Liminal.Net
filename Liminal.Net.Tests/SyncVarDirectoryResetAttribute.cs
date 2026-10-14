using Liminal.Net.SyncVar.Internal;
using NUnit.Framework;
using NUnit.Framework.Interfaces;
using System;

[assembly: Liminal.Net.Tests.SyncVarDirectoryReset]

namespace Liminal.Net.Tests
{
    //Syncvars are listed process wide for adoption, reset per test so leftovers from earlier tests dont get adopted
    [AttributeUsage(AttributeTargets.Assembly)]
    public sealed class SyncVarDirectoryResetAttribute : Attribute, ITestAction
    {
        public ActionTargets Targets => ActionTargets.Test;

        public void BeforeTest(ITest test) => SyncVarDirectory.ResetForTesting();

        public void AfterTest(ITest test)
        {
        }
    }
}
