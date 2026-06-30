py_install_dir = py.get_install_dir()

install_subdir(
  'flux_fiction',
  install_dir: py_install_dir,
  exclude_directories: ['__pycache__'],
)

cc = meson.get_compiler('c')
jansson_dep = dependency('jansson', required: true)

flux_prefix = get_option('flux_prefix')
use_system_flux = get_option('use_system_flux')

flux_candidates = []
if use_system_flux
  flux_candidates += [
    {
      'name': 'system',
      'include_dirs': ['/usr/include', '/usr/local/include'],
      'library_dirs': ['/usr/lib', '/usr/local/lib', '/usr/lib64', '/usr/local/lib64'],
    },
  ]
endif

if flux_prefix != ''
  flux_candidates += [
    {
      'name': 'flux_prefix',
      'include_dirs': [join_paths(flux_prefix, 'include')],
      'library_dirs': [join_paths(flux_prefix, 'lib'), join_paths(flux_prefix, 'lib64')],
    },
  ]
endif

workspace_flux_prefix = join_paths(meson.project_source_root(), '..', 'container-installs', 'flux-core')
flux_candidates += [
  {
    'name': 'workspace_prefix',
    'include_dirs': [join_paths(workspace_flux_prefix, 'include')],
    'library_dirs': [join_paths(workspace_flux_prefix, 'lib'), join_paths(workspace_flux_prefix, 'lib64')],
  },
]

workspace_flux_root = join_paths(meson.project_source_root(), '..', 'flux-core')
flux_candidates += [
  {
    'name': 'workspace_checkout',
    'include_dirs': [join_paths(workspace_flux_root, 'src', 'include')],
    'library_dirs': [join_paths(workspace_flux_root, 'src', 'common', '.libs')],
  },
]

flux_include_dir = ''
flux_library_dir = ''
flux_core_dep = disabler()

foreach candidate : flux_candidates
  if flux_core_dep.found() and flux_include_dir != ''
    break
  endif

  candidate_dep = disabler()
  candidate_libdir = ''
  foreach libdir : candidate['library_dirs']
    candidate_dep = cc.find_library('flux-core', dirs: [libdir], required: false)
    if candidate_dep.found()
      candidate_libdir = libdir
      break
    endif
  endforeach

  candidate_incdir = ''
  foreach incdir : candidate['include_dirs']
    if cc.has_header('flux/core.h', args: ['-I' + incdir])
      candidate_incdir = incdir
      break
    endif
  endforeach

  if candidate_dep.found() and candidate_incdir != ''
    flux_core_dep = candidate_dep
    flux_include_dir = candidate_incdir
    flux_library_dir = candidate_libdir
  endif
endforeach

if not flux_core_dep.found() or flux_include_dir == ''
  error('Could not find flux-core headers and libraries. Set -Dflux_prefix=/path/to/flux install if needed.')
endif

jobtap_rpath = flux_library_dir

shared_module(
  'emu-jobtap',
  [
    'jobtap/emu-jobtap.c',
    'jobtap/ff_otel.c',
  ],
  dependencies: [
    flux_core_dep,
    jansson_dep,
  ],
  include_directories: [
    include_directories('jobtap'),
  ],
  c_args: [
    '-Wall',
    '-Wextra',
    '-Werror=implicit-function-declaration',
    '-I' + flux_include_dir,
  ],
  name_prefix: '',
  install: true,
  install_dir: join_paths(py_install_dir, 'flux_fiction', '_native'),
  install_rpath: jobtap_rpath,
)
