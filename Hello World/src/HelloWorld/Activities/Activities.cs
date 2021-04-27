using System;
using System.Collections;
using System.Runtime.CompilerServices;
using System.Linq;

[assembly: InternalsVisibleTo("../Function")]

namespace AWSPipe
{
    public static class Activities
    {
        public static Func<IEnumerable, IEnumerable> eachMaker(Action<object> callback = null)
        {
            IEnumerable returnFunc(IEnumerable source)
            {
                foreach (object r in source)
                {
                    if (callback != null)
                    {
                        callback(r);
                    }
                    yield return r;
                }
            };
            return returnFunc;
        }

        public static Func<IEnumerable, IEnumerable> filterMaker<T>(Func<T, bool> callback)
        {
            IEnumerable returnFunc(IEnumerable source)
            {
                foreach (T r in source)
                {
                    if (callback(r))
                    {
                        yield return r;
                    }
                }
            };
            return returnFunc;
        }

        public static Func<IEnumerable, IEnumerable> mapMaker<T, U>(Func<T, U> callback)
        {
            IEnumerable returnFunc(IEnumerable source)
            {
                foreach (T r in source)
                {
                    yield return callback(r);
                }
            };
            return returnFunc;
        }

        public static Func<IEnumerable, IEnumerable> topNMaker<T>(int n)
        {
            IEnumerable returnFunc(IEnumerable source)
            {
                int i = 0;
                foreach (T r in source)
                {

                    yield return r;
                    if (++i >= n)
                    {
                        break;
                    }
                }
            }
            return returnFunc;
        }

        public static Func<IEnumerable, IEnumerable> concatMaker<T>(IEnumerable concatSource)
        {
            IEnumerable returnFunc(IEnumerable source)
            {
                foreach (T r in source)
                {
                    yield return r;
                    yield return concatSource;
                }
            }
            return returnFunc;
        }

        public static Func<IEnumerable, IEnumerable> enumumerateMaker<T>()
        {
            IEnumerable returnFunc(IEnumerable source)
            {
                int i = -1;
                foreach (T r in source)
                {
                    yield return new Tuple<int, T>(++i, r);
                }
            }
            return returnFunc;
        }

        public static Func<IEnumerable, IEnumerable> skipMaker<T>(int n)
        {
            IEnumerable returnFunc(IEnumerable source)
            {
                int i = -1;
                foreach (T r in source)
                {
                    if (++i >= 0)
                    {
                        yield return r;
                    }
                }
            }
            return returnFunc;
        }

        public static Func<ArrayList, ArrayList> sortMaker<T>(IComparer comparison)
        {
            // Blocking implementation will require data to be buffered into an array first
            ArrayList returnFunc(ArrayList source)
            {
                source.Sort(comparison);
                return source;
            }
            return returnFunc;
        }

        public static Func<IEnumerable, IEnumerable> pipelineMaker(params Func<IEnumerable, IEnumerable>[] a)
        {
            IEnumerable returnFunc(IEnumerable source)
            {
                IEnumerable tail = source;
                foreach (Func<IEnumerable, IEnumerable> activity in a)
                {
                    tail = activity.Invoke(tail);
                }
                return tail;
            }
            return returnFunc;
        }
    }
}
