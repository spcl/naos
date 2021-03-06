/*
 * Copyright (c) 2015, 2018, Red Hat, Inc. All rights reserved.
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

#include "precompiled.hpp"

#include "classfile/classLoaderData.hpp"
#include "classfile/stringTable.hpp"
#include "classfile/systemDictionary.hpp"
#include "code/codeCache.hpp"
#include "gc/shenandoah/shenandoahClosures.inline.hpp"
#include "gc/shenandoah/shenandoahRootProcessor.inline.hpp"
#include "gc/shenandoah/shenandoahHeap.hpp"
#include "gc/shenandoah/shenandoahHeuristics.hpp"
#include "gc/shenandoah/shenandoahPhaseTimings.hpp"
#include "gc/shenandoah/shenandoahStringDedup.hpp"
#include "gc/shenandoah/shenandoahTimingTracker.hpp"
#include "gc/shenandoah/shenandoahVMOperations.hpp"
#include "gc/shared/weakProcessor.hpp"
#include "memory/allocation.inline.hpp"
#include "memory/iterator.hpp"
#include "memory/resourceArea.hpp"
#include "runtime/thread.hpp"
#include "services/management.hpp"

ShenandoahSerialRoot::ShenandoahSerialRoot(ShenandoahSerialRoot::OopsDo oops_do, ShenandoahPhaseTimings::GCParPhases phase) :
  _claimed(false), _oops_do(oops_do), _phase(phase) {
}

void ShenandoahSerialRoot::oops_do(OopClosure* cl, uint worker_id) {
  if (!_claimed && Atomic::cmpxchg(true, &_claimed, false) == false) {
    ShenandoahWorkerTimings* worker_times = ShenandoahHeap::heap()->phase_timings()->worker_times();
    ShenandoahWorkerTimingsTracker timer(worker_times, _phase, worker_id);
    _oops_do(cl);
  }
}

ShenandoahSerialRoots::ShenandoahSerialRoots() :
  _universe_root(&ShenandoahSerialRoots::universe_oops_do, ShenandoahPhaseTimings::UniverseRoots),
  _object_synchronizer_root(&ObjectSynchronizer::oops_do, ShenandoahPhaseTimings::ObjectSynchronizerRoots),
  _management_root(&Management::oops_do, ShenandoahPhaseTimings::ManagementRoots),
  _system_dictionary_root(&SystemDictionary::oops_do, ShenandoahPhaseTimings::SystemDictionaryRoots),
  _jvmti_root(&JvmtiExport::oops_do, ShenandoahPhaseTimings::JVMTIRoots) {
}

void ShenandoahSerialRoots::oops_do(OopClosure* cl, uint worker_id) {
  _universe_root.oops_do(cl, worker_id);
  _object_synchronizer_root.oops_do(cl, worker_id);
  _management_root.oops_do(cl, worker_id);
  _system_dictionary_root.oops_do(cl, worker_id);
  _jvmti_root.oops_do(cl, worker_id);
}

ShenandoahJNIHandleRoots::ShenandoahJNIHandleRoots() :
  ShenandoahSerialRoot(&JNIHandles::oops_do, ShenandoahPhaseTimings::JNIRoots) {
}

ShenandoahThreadRoots::ShenandoahThreadRoots(bool is_par) : _is_par(is_par) {
  Threads::change_thread_claim_parity();
}

void ShenandoahThreadRoots::oops_do(OopClosure* oops_cl, CodeBlobClosure* code_cl, uint worker_id) {
  ShenandoahWorkerTimings* worker_times = ShenandoahHeap::heap()->phase_timings()->worker_times();
  ShenandoahWorkerTimingsTracker timer(worker_times, ShenandoahPhaseTimings::ThreadRoots, worker_id);
  ResourceMark rm;
  Threads::possibly_parallel_oops_do(_is_par, oops_cl, code_cl);
}

void ShenandoahThreadRoots::threads_do(ThreadClosure* tc, uint worker_id) {
  ShenandoahWorkerTimings* worker_times = ShenandoahHeap::heap()->phase_timings()->worker_times();
  ShenandoahWorkerTimingsTracker timer(worker_times, ShenandoahPhaseTimings::ThreadRoots, worker_id);
  ResourceMark rm;
  Threads::possibly_parallel_threads_do(_is_par, tc);
}

ShenandoahThreadRoots::~ShenandoahThreadRoots() {
  Threads::assert_all_threads_claimed();
}

ShenandoahWeakRoots::ShenandoahWeakRoots(uint n_workers) :
  _par_state_string(StringTable::weak_storage()),
  _claimed(false) {
}

ShenandoahWeakRoots::~ShenandoahWeakRoots() {
}

ShenandoahStringDedupRoots::ShenandoahStringDedupRoots() {
  if (ShenandoahStringDedup::is_enabled()) {
    StringDedup::gc_prologue(false);
  }
}

ShenandoahStringDedupRoots::~ShenandoahStringDedupRoots() {
  if (ShenandoahStringDedup::is_enabled()) {
    StringDedup::gc_epilogue();
  }
}

void ShenandoahStringDedupRoots::oops_do(BoolObjectClosure* is_alive, OopClosure* keep_alive, uint worker_id) {
  if (ShenandoahStringDedup::is_enabled()) {
    ShenandoahStringDedup::parallel_oops_do(is_alive, keep_alive, worker_id);
  }
}

ShenandoahRootProcessor::ShenandoahRootProcessor(ShenandoahPhaseTimings::Phase phase) :
  _heap(ShenandoahHeap::heap()),
  _phase(phase) {
  assert(SafepointSynchronize::is_at_safepoint(), "Must at safepoint");
  _heap->phase_timings()->record_workers_start(_phase);
}

ShenandoahRootProcessor::~ShenandoahRootProcessor() {
  assert(SafepointSynchronize::is_at_safepoint(), "Must at safepoint");
  _heap->phase_timings()->record_workers_end(_phase);
}

ShenandoahRootEvacuator::ShenandoahRootEvacuator(uint n_workers, ShenandoahPhaseTimings::Phase phase) :
  ShenandoahRootProcessor(phase),
  _thread_roots(n_workers > 1),
  _weak_roots(n_workers) {
}

void ShenandoahRootEvacuator::roots_do(uint worker_id, OopClosure* oops) {
  MarkingCodeBlobClosure blobsCl(oops, CodeBlobToOopClosure::FixRelocations);
  CLDToOopClosure clds(oops);
  CLDToOopClosure* weak_clds = ShenandoahHeap::heap()->unload_classes() ? NULL : &clds;

  AlwaysTrueClosure always_true;

  _serial_roots.oops_do(oops, worker_id);
  _jni_roots.oops_do(oops, worker_id);

  _thread_roots.oops_do(oops, NULL, worker_id);
  _cld_roots.cld_do(&clds, worker_id);
  _code_roots.code_blobs_do(&blobsCl, worker_id);

  _weak_roots.oops_do<AlwaysTrueClosure, OopClosure>(&always_true, oops, worker_id);
  _dedup_roots.oops_do(&always_true, oops, worker_id);
}

ShenandoahRootUpdater::ShenandoahRootUpdater(uint n_workers, ShenandoahPhaseTimings::Phase phase, bool update_code_cache) :
  ShenandoahRootProcessor(phase),
  _thread_roots(n_workers > 1),
  _weak_roots(n_workers),
  _update_code_cache(update_code_cache) {
}

ShenandoahRootAdjuster::ShenandoahRootAdjuster(uint n_workers, ShenandoahPhaseTimings::Phase phase) :
  ShenandoahRootProcessor(phase),
  _thread_roots(n_workers > 1),
  _weak_roots(n_workers) {
  assert(ShenandoahHeap::heap()->is_full_gc_in_progress(), "Full GC only");
}

void ShenandoahRootAdjuster::roots_do(uint worker_id, OopClosure* oops) {
  CodeBlobToOopClosure adjust_code_closure(oops, CodeBlobToOopClosure::FixRelocations);
  CLDToOopClosure adjust_cld_closure(oops);
  AlwaysTrueClosure always_true;

  _serial_roots.oops_do(oops, worker_id);
  _jni_roots.oops_do(oops, worker_id);

  _thread_roots.oops_do(oops, NULL, worker_id);
  _cld_roots.cld_do(&adjust_cld_closure, worker_id);
  _code_roots.code_blobs_do(&adjust_code_closure, worker_id);

  _weak_roots.oops_do<AlwaysTrueClosure, OopClosure>(&always_true, oops, worker_id);
  _dedup_roots.oops_do(&always_true, oops, worker_id);
}

 ShenandoahHeapIterationRootScanner::ShenandoahHeapIterationRootScanner() :
   ShenandoahRootProcessor(ShenandoahPhaseTimings::_num_phases),
   _thread_roots(false /*is par*/),
   _weak_roots(1) {
 }

 void ShenandoahHeapIterationRootScanner::roots_do(OopClosure* oops) {
   assert(Thread::current()->is_VM_thread(), "Only by VM thread");
   // Must use _claim_none to avoid interfering with concurrent CLDG iteration
   CLDToOopClosure clds(oops, false);
   MarkingCodeBlobClosure code(oops, !CodeBlobToOopClosure::FixRelocations);
   ShenandoahParallelOopsDoThreadClosure tc_cl(oops, &code, NULL);
   AlwaysTrueClosure always_true;
   ResourceMark rm;

   _serial_roots.oops_do(oops, 0);
   _jni_roots.oops_do(oops, 0);
   _cld_roots.cld_do(&clds, 0);
   _thread_roots.threads_do(&tc_cl, 0);
   _code_roots.code_blobs_do(&code, 0);

   _weak_roots.oops_do<AlwaysTrueClosure, OopClosure>(&always_true, oops, 0);
   _dedup_roots.oops_do(&always_true, oops, 0);
 }

 void ShenandoahHeapIterationRootScanner::strong_roots_do(OopClosure* oops) {
   assert(Thread::current()->is_VM_thread(), "Only by VM thread");
   // Must use _claim_none to avoid interfering with concurrent CLDG iteration
   CLDToOopClosure clds(oops, false);
   MarkingCodeBlobClosure code(oops, !CodeBlobToOopClosure::FixRelocations);
   ShenandoahParallelOopsDoThreadClosure tc_cl(oops, &code, NULL);
   ResourceMark rm;

   _serial_roots.oops_do(oops, 0);
   _jni_roots.oops_do(oops, 0);
   _cld_roots.always_strong_cld_do(&clds, 0);
   _thread_roots.threads_do(&tc_cl, 0);
 }
