/*
 * Copyright (c) 2022 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.pool.introspection.micrometer;

import io.micrometer.common.docs.KeyName;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.docs.DocumentedMeter;

/**
 * Meters used by {@link Micrometer} utility.
 */
enum DocumentedPoolMeters implements DocumentedMeter {

	ACQUIRED {
		@Override
		public String getName() {
			return "%s.resources.acquired";
		}

		@Override
		public Meter.Type getType() {
			return Meter.Type.GAUGE;
		}
	},
	ALLOCATED {
		@Override
		public String getName() {
			return "%s.resources.allocated";
		}

		@Override
		public Meter.Type getType() {
			return Meter.Type.GAUGE;
		}
	},
	IDLE {
		@Override
		public String getName() {
			return "%s.resources.idle";
		}

		@Override
		public Meter.Type getType() {
			return Meter.Type.GAUGE;
		}
	},
	PENDING_ACQUIRE {
		@Override
		public String getName() {
			return "%s.resources.pendingAcquire";
		}

		@Override
		public Meter.Type getType() {
			return Meter.Type.GAUGE;
		}
	},


	ALLOCATION {
		@Override
		public String getName() {
			return "%s.allocation";
		}

		@Override
		public Meter.Type getType() {
			return Meter.Type.TIMER;
		}

		@Override
		public KeyName[] getKeyNames() {
			return AllocationTags.values();
		}
	},

	DESTROYED {
		@Override
		public String getName() {
			return "%s.destroyed";
		}

		@Override
		public Meter.Type getType() {
			return Meter.Type.TIMER;
		}
	},

	SUMMARY_IDLENESS {
		@Override
		public String getName() {
			return "%s.resources.summary.idleness";
		}

		@Override
		public Meter.Type getType() {
			return Meter.Type.TIMER;
		}
	},

	SUMMARY_LIFETIME {
		@Override
		public String getName() {
			return "%s.resources.summary.lifetime";
		}

		@Override
		public Meter.Type getType() {
			return Meter.Type.TIMER;
		}
	},

	RECYCLED {
		@Override
		public String getName() {
			return "%s.recycled";
		}

		@Override
		public Meter.Type getType() {
			return Meter.Type.COUNTER;
		}
	},

	RECYCLED_NOTABLE {
		@Override
		public String getName() {
			return "%s.recycled.notable";
		}

		@Override
		public Meter.Type getType() {
			return Meter.Type.COUNTER;
		}

		@Override
		public KeyName[] getKeyNames() {
			return RecycledNotableTags.values();
		}
	},


	RESET {
		@Override
		public String getName() {
			return "%s.reset";
		}

		@Override
		public Meter.Type getType() {
			return Meter.Type.TIMER;
		}
	};

	public enum AllocationTags implements KeyName {

		/**
		 * Indicates whether the allocation timed was a {@code success} or {@code failure}.
		 */
		OUTCOME {
			@Override
			public String asString() {
				return "pool.allocation.outcome";
			}
		};

		public static final Tag OUTCOME_SUCCESS = Tag.of(OUTCOME.asString(), "success");
		public static final Tag OUTCOME_FAILURE = Tag.of(OUTCOME.asString(), "failure");

	}

	public enum RecycledNotableTags implements KeyName {

		/**
		 * Indicates that a notable recycling path was used (as opposed to the common
		 * one): either the {@code slow} path or the {@code fast} path.
		 */
		PATH {
			@Override
			public String asString() {
				return "pool.recycling.path";
			}
		};

		public static final Tag PATH_SLOW = Tag.of(PATH.asString(), "slow");
		public static final Tag PATH_FAST = Tag.of(PATH.asString(), "fast");


	}
}
