"""
ZFS Pool Management Service
Handles zpool operations: list, status, create, destroy, scrub, etc.
"""
import re
import os
import subprocess
from typing import List, Dict, Any, Optional
from datetime import datetime
from config.settings import Settings

# Try to import libzfs_core, but fall back to shell commands if not available
try:
    import libzfs_core as lzc
    HAS_LIBZFS_CORE = True
except ImportError:
    HAS_LIBZFS_CORE = False


class ZFSPoolService:
    """Service for managing ZFS pools using libzfs_core and shell commands"""
    
    # ZFS naming pattern: alphanumeric, underscore, hyphen, period, colon
    ZFS_POOL_NAME_PATTERN = re.compile(r'^[a-zA-Z0-9][a-zA-Z0-9_\-.:]*$')
    
    def __init__(self):
        """Initialize the ZFS Pool Service with settings"""
        self.settings = Settings()
        self.timeouts = self.settings.ZPOOL_TIMEOUTS

    def _run_zfs_command(self, args: List[str], timeout_key: str = 'default', check: bool = True) -> subprocess.CompletedProcess:
        """
        Helper to run ZFS commands with automatic sudo handling and centralized error management.
        
        Args:
            args: The command arguments (e.g., ['zpool', 'list'])
            timeout_key: Key to look up timeout in settings (e.g., 'list', 'status')
            check: Whether to raise CalledProcessError on non-zero exit code
        """
        timeout = self.timeouts.get(timeout_key, self.timeouts['default'])
        
        # Original Creator Intent: This is where we would switch to libzfs logic
        # if HAS_LIBZFS_CORE and self.prefer_bindings:
        #     return self._run_libzfs_command(...)

        # Fallback / Shell Logic
        cmd = list(args)
        
        # Check if we are running as root; if not, prepend sudo
        if os.geteuid() != 0:
            cmd.insert(0, 'sudo')

        try:
            return subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=check,
                timeout=timeout
            )
        except subprocess.TimeoutExpired:
            cmd_str = " ".join(cmd)
            raise Exception(
                f"Command '{cmd_str}' timed out after {timeout} seconds. "
                "The system may be unresponsive or the pool is busy."
            )
        except subprocess.CalledProcessError as e:
            # Re-raise with a clear message, preserving the stderr from ZFS
            raise Exception(f"ZFS command failed: {e.stderr.strip()}")

    @classmethod
    def validate_pool_name(cls, pool_name: str) -> None:
        """Validate a ZFS pool name against naming rules."""
        if not pool_name:
            raise ValueError("Pool name cannot be empty")
        
        if not cls.ZFS_POOL_NAME_PATTERN.match(pool_name):
            raise ValueError(
                f"Invalid pool name '{pool_name}'. Pool names must start with an alphanumeric "
                "character and contain only alphanumeric characters, underscores, hyphens, "
                "periods, or colons."
            )
    
    def list_pools(self) -> List[Dict[str, Any]]:
        """List all ZFS pools with their key properties"""
        result = self._run_zfs_command(
            ['zpool', 'list', '-H', '-o', 'name,size,alloc,free,frag,cap,dedup,health,altroot'],
            timeout_key='list'
        )
        
        pools = []
        for line in result.stdout.strip().split('\n'):
            if not line:
                continue
                
            parts = line.split('\t')
            if len(parts) >= 9:
                pools.append({
                    'name': parts[0],
                    'size': parts[1],
                    'alloc': parts[2],
                    'free': parts[3],
                    'frag': parts[4],
                    'cap': parts[5],
                    'dedup': parts[6],
                    'health': parts[7],
                    'altroot': parts[8] if parts[8] != '-' else None
                })
        
        return pools
    
    def get_pool_status(self, pool_name: str) -> Dict[str, Any]:
        """Get detailed status for a specific pool"""
        self.validate_pool_name(pool_name)
        
        # Get detailed status
        status_result = self._run_zfs_command(
            ['zpool', 'status', pool_name],
            timeout_key='status'
        )
        
        # Get pool properties
        props_result = self._run_zfs_command(
            ['zpool', 'get', '-H', 'all', pool_name],
            timeout_key='properties'
        )
        
        # Parse properties
        properties = {}
        for line in props_result.stdout.strip().split('\n'):
            if not line:
                continue
            parts = line.split('\t')
            if len(parts) >= 3:
                properties[parts[1]] = {
                    'value': parts[2],
                    'source': parts[3] if len(parts) > 3 else 'default'
                }
        
        return {
            'name': pool_name,
            'status_output': status_result.stdout,
            'properties': properties
        }
    
    def get_pool_iostat(self, pool_name: Optional[str] = None, verbose: bool = False) -> Dict[str, Any]:
        """Get I/O statistics for pools"""
        if pool_name:
            self.validate_pool_name(pool_name)
            
        cmd = ['zpool', 'iostat', '-H']
        if verbose:
            cmd.append('-v')
        if pool_name:
            cmd.append(pool_name)
        
        result = self._run_zfs_command(cmd, timeout_key='iostat')
        
        return {
            'output': result.stdout,
            'timestamp': datetime.now().isoformat()
        }
    
    def scrub_pool(self, pool_name: str) -> None:
        """Start a scrub on the specified pool"""
        self.validate_pool_name(pool_name)
        self._run_zfs_command(['zpool', 'scrub', pool_name], timeout_key='scrub')
    
    def stop_scrub(self, pool_name: str) -> None:
        """Stop a running scrub on the specified pool"""
        self.validate_pool_name(pool_name)
        self._run_zfs_command(['zpool', 'scrub', '-s', pool_name], timeout_key='scrub')
    
    def export_pool(self, pool_name: str, force: bool = False) -> None:
        """Export a ZFS pool"""
        self.validate_pool_name(pool_name)
        cmd = ['zpool', 'export']
        if force:
            cmd.append('-f')
        cmd.append(pool_name)
        
        self._run_zfs_command(cmd, timeout_key='export')
    
    def import_pool(self, pool_name: str, force: bool = False, 
                   altroot: Optional[str] = None) -> None:
        """Import a ZFS pool"""
        self.validate_pool_name(pool_name)
        cmd = ['zpool', 'import']
        if force:
            cmd.append('-f')
        if altroot:
            cmd.extend(['-R', altroot])
        cmd.append(pool_name)
        
        self._run_zfs_command(cmd, timeout_key='import')
    
    def get_pool_history(self, pool_name: str, internal: bool = False, 
                        limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get command history for a pool"""
        self.validate_pool_name(pool_name)
        cmd = ['zpool', 'history', '-l']
        if internal:
            cmd.append('-i')
        cmd.append(pool_name)
        
        result = self._run_zfs_command(cmd, timeout_key='history')
        
        history = []
        for line in result.stdout.strip().split('\n'):
            if not line:
                continue
            history.append({'entry': line})
        
        if limit:
            history = history[-limit:]
        
        return history
    
    def create_pool(self, pool_name: str, vdevs: List[str], 
                   properties: Optional[Dict[str, str]] = None,
                   force: bool = False) -> None:
        """Create a new ZFS pool"""
        self.validate_pool_name(pool_name)
        cmd = ['zpool', 'create']
        
        if force:
            cmd.append('-f')
        
        # Add properties
        if properties:
            for key, value in properties.items():
                cmd.extend(['-o', f'{key}={value}'])
        
        cmd.append(pool_name)
        cmd.extend(vdevs)
        
        self._run_zfs_command(cmd, timeout_key='create')
    
    def destroy_pool(self, pool_name: str, force: bool = False) -> None:
        """Destroy a ZFS pool (WARNING: Destructive)"""
        self.validate_pool_name(pool_name)
        cmd = ['zpool', 'destroy']
        if force:
            cmd.append('-f')
        cmd.append(pool_name)
        
        self._run_zfs_command(cmd, timeout_key='destroy')
    
    def set_pool_property(self, pool_name: str, property_name: str, 
                         property_value: str) -> None:
        """Set a property on a pool"""
        self.validate_pool_name(pool_name)
        self._run_zfs_command(
            ['zpool', 'set', f'{property_name}={property_value}', pool_name],
            timeout_key='properties'
        )
    
    def get_importable_pools(self) -> List[Dict[str, Any]]:
        """List pools available for import"""
        # check=False because 'zpool import' returns non-zero if no pools are found
        # checking safely inside the method
        try:
            result = self._run_zfs_command(
                ['zpool', 'import'], 
                timeout_key='import',
                check=False
            )
        except Exception as e:
            # run_zfs_command might still throw if it's a timeout
            raise e

        # Parse the output to extract pool information
        pools = []
        current_pool = None
        
        for line in result.stdout.split('\n'):
            line = line.strip()
            if line.startswith('pool:'):
                if current_pool:
                    pools.append(current_pool)
                current_pool = {'name': line.split(':', 1)[1].strip()}
            elif current_pool and line.startswith('id:'):
                current_pool['id'] = line.split(':', 1)[1].strip()
            elif current_pool and line.startswith('state:'):
                current_pool['state'] = line.split(':', 1)[1].strip()
        
        if current_pool:
            pools.append(current_pool)
        
        return pools