/*
 * Copyright (c) 2019, Red Hat, Inc. All rights reserved.
 *
 * This code is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 only, as
 * published by the Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Please contact Oracle, 500 Oracle Parkway, Redwood Shores, CA 94065 USA
 * or visit www.oracle.com if you need additional information or have any
 * questions.
 *
 */

#ifndef SHARE_GC_SHENANDOAH_SHENANDOAHROOTPROCESSOR_INLINE_HPP
#define SHARE_GC_SHENANDOAH_SHENANDOAHROOTPROCESSOR_INLINE_HPP

#include "classfile/stringTable.hpp"
#include "gc/shenandoah/shenandoahHeuristics.hpp"
#include "gc/shenandoah/shenandoahRootProcessor.hpp"
#include "gc/shenandoah/shenandoahTimingTracker.hpp"
#include "memory/resourceArea.hpp"
#include "runtime/safepoint.hpp"

template <typename IsAlive, typename KeepAlive>
void ShenandoahWeakRoots::oops_do(IsAlive* is_alive, KeepAlive* keep_alive, uint worker_id) {
  if (!_claimed && Atomic::cmpxchg(true, &_claimed, false) == false) {
    WeakProcessor::weak_oops_do(is_alive, keep_alive);
  }
  StringTable::possibly_parallel_oops_do(&_par_state_string, keep_alive);
}

template <bool SINGLE_THREADED>
ShenandoahClassLoaderDataRoots<SINGLE_THREADED>::ShenandoahClassLoaderDataRoots() {
  if (!SINGLE_THREADED) {
    ClassLoaderDataGraph::clear_claimed_marks();
  }
}

template <bool SINGLE_THREADED>
void ShenandoahClassLoaderDataRoots<SINGLE_THREADED>::always_strong_cld_do(CLDClosure* clds, uint worker_id) {
  if (SINGLE_THREADED) {
    assert(SafepointSynchronize::is_at_safepoint(), "Must be at a safepoint");
    assert(Thread::current()->is_VM_thread(), "Single threaded CLDG iteration can only be done by VM thread");
    ClassLoaderDataGraph::always_strong_cld_do(clds);
  } else {
   ShenandoahWorkerTimings* worker_times = ShenandoahHeap::heap()->phase_timings()->worker_times();
   ShenandoahWorkerTimingsTracker timer(worker_times, ShenandoahPhaseTimings::CLDGRoots, worker_id);
   ClassLoaderDataGraph::always_strong_cld_do(clds);
  }
}

template <bool SINGLE_THREADED>
void ShenandoahClassLoaderDataRoots<SINGLE_THREADED>::cld_do(CLDClosure* clds, uint worker_id) {
  if (SINGLE_THREADED) {
    assert(SafepointSynchronize::is_at_safepoint(), "Must be at a safepoint");
    assert(Thread::current()->is_VM_thread(), "Single threaded CLDG iteration can only be done by VM thread");
    ClassLoaderDataGraph::cld_do(clds);
  } else {
    ShenandoahWorkerTimings* worker_times = ShenandoahHeap::heap()->phase_timings()->worker_times();
    ShenandoahWorkerTimingsTracker timer(worker_times, ShenandoahPhaseTimings::CLDGRoots, worker_id);
    ClassLoaderDataGraph::cld_do(clds);
  }
}

template <typename ITR>
ShenandoahCodeCacheRoots<ITR>::ShenandoahCodeCacheRoots() {
  nmethod::oops_do_marking_prologue();
}

template <typename ITR>
void ShenandoahCodeCacheRoots<ITR>::code_blobs_do(CodeBlobClosure* blob_cl, uint worker_id) {
  ShenandoahWorkerTimings* worker_times = ShenandoahHeap::heap()->phase_timings()->worker_times();
  ShenandoahWorkerTimingsTracker timer(worker_times, ShenandoahPhaseTimings::CodeCacheRoots, worker_id);
  _coderoots_iterator.possibly_parallel_blobs_do(blob_cl);
}

template <typename ITR>
ShenandoahCodeCacheRoots<ITR>::~ShenandoahCodeCacheRoots() {
  nmethod::oops_do_marking_epilogue();
}

class ShenandoahParallelOopsDoThreadClosure : public ThreadClosure {
private:
  OopClosure* _f;
  CodeBlobClosure* _cf;
  ThreadClosure* _thread_cl;
public:
  ShenandoahParallelOopsDoThreadClosure(OopClosure* f, CodeBlobClosure* cf, ThreadClosure* thread_cl) :
    _f(f), _cf(cf), _thread_cl(thread_cl) {}

  void do_thread(Thread* t) {
    if (_thread_cl != NULL) {
      _thread_cl->do_thread(t);
    }
    t->oops_do(_f, _cf);
  }
};

template <typename ITR>
ShenandoahRootScanner<ITR>::ShenandoahRootScanner(uint n_workers, ShenandoahPhaseTimings::Phase phase) :
  ShenandoahRootProcessor(phase),
  _thread_roots(n_workers > 1) {
}

template <typename ITR>
void ShenandoahRootScanner<ITR>::roots_do(uint worker_id, OopClosure* oops) {
  CLDToOopClosure clds_cl(oops);
  MarkingCodeBlobClosure blobs_cl(oops, !CodeBlobToOopClosure::FixRelocations);
  roots_do(worker_id, oops, &clds_cl, &blobs_cl);
}

template <typename ITR>
void ShenandoahRootScanner<ITR>::strong_roots_do(uint worker_id, OopClosure* oops) {
  CLDToOopClosure clds_cl(oops);
  MarkingCodeBlobClosure blobs_cl(oops, !CodeBlobToOopClosure::FixRelocations);
  strong_roots_do(worker_id, oops, &clds_cl, &blobs_cl);
}

template <typename ITR>
void ShenandoahRootScanner<ITR>::roots_do(uint worker_id, OopClosure* oops, CLDClosure* clds, CodeBlobClosure* code, ThreadClosure *tc) {
  assert(!ShenandoahHeap::heap()->unload_classes() ||
          ShenandoahHeap::heap()->is_traversal_mode(),
          "No class unloading or traversal GC");
  ShenandoahParallelOopsDoThreadClosure tc_cl(oops, code, tc);
  ResourceMark rm;

  _serial_roots.oops_do(oops, worker_id);
  _jni_roots.oops_do(oops, worker_id);

  if (clds != NULL) {
    _cld_roots.cld_do(clds, worker_id);
  } else {
    assert(ShenandoahHeap::heap()->is_concurrent_traversal_in_progress(), "Only possible with traversal GC");
  }

  _thread_roots.threads_do(&tc_cl, worker_id);

  // With ShenandoahConcurrentScanCodeRoots, we avoid scanning the entire code cache here,
  // and instead do that in concurrent phase under the relevant lock. This saves init mark
  // pause time.
  if (code != NULL && !ShenandoahConcurrentScanCodeRoots) {
    _code_roots.code_blobs_do(code, worker_id);
  }
}

template <typename ITR>
void ShenandoahRootScanner<ITR>::strong_roots_do(uint worker_id, OopClosure* oops, CLDClosure* clds, CodeBlobClosure* code, ThreadClosure* tc) {
  assert(ShenandoahHeap::heap()->unload_classes(), "Should be used during class unloading");
  ShenandoahParallelOopsDoThreadClosure tc_cl(oops, code, tc);
  ResourceMark rm;

  _serial_roots.oops_do(oops, worker_id);
  _jni_roots.oops_do(oops, worker_id);
  _cld_roots.always_strong_cld_do(clds, worker_id);
  _thread_roots.threads_do(&tc_cl, worker_id);
}

template <typename IsAlive, typename KeepAlive>
void ShenandoahRootUpdater::roots_do(uint worker_id, IsAlive* is_alive, KeepAlive* keep_alive) {
  CodeBlobToOopClosure update_blobs(keep_alive, CodeBlobToOopClosure::FixRelocations);
  CLDToOopClosure clds(keep_alive);

  _serial_roots.oops_do(keep_alive, worker_id);
  _jni_roots.oops_do(keep_alive, worker_id);

  _thread_roots.oops_do(keep_alive, NULL, worker_id);
  _cld_roots.cld_do(&clds, worker_id);

  if(_update_code_cache) {
    _code_roots.code_blobs_do(&update_blobs, worker_id);
  }

  _weak_roots.oops_do<IsAlive, KeepAlive>(is_alive, keep_alive, worker_id);
  _dedup_roots.oops_do(is_alive, keep_alive, worker_id);
}

#endif // SHARE_GC_SHENANDOAH_SHENANDOAHROOTPROCESSOR_INLINE_HPP
