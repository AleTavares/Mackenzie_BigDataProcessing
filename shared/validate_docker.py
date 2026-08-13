#!/usr/bin/env python3
"""
Validação dos arquivos Docker Compose do curso Big Data Processing.
Verifica:
1. Sintaxe YAML válida
2. Imagens referenciadas corretas
3. Conflitos de porta
4. Consistência de configuração de rede
"""

import sys
import os

# Tentativa de importar yaml
try:
    import yaml
except ImportError:
    print("❌ Módulo 'yaml' (PyYAML) não encontrado. Instalando...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pyyaml", "-q"])
    import yaml

# Diretório deste script
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Arquivos a validar
COMPOSE_FILES = [
    os.path.join(SCRIPT_DIR, "docker-compose.yml"),
    os.path.join(SCRIPT_DIR, "docker-compose.airflow.yml"),
    os.path.join(SCRIPT_DIR, "docker-compose.full.yml"),
]

# Imagens esperadas conforme o design
EXPECTED_IMAGES = [
    "bitnami/spark:3.5",
    "apache/airflow:2.8-python3.11",
    "jupyter/pyspark-notebook:latest",
]

# Portas esperadas conforme requisito 4.6
EXPECTED_PORTS = {
    "Spark UI": 8080,
    "Airflow": 8081,
    "Jupyter": 8888,
}


def validate_yaml_syntax():
    """Validação 1: Sintaxe YAML de todos os 3 arquivos compose."""
    print("=" * 70)
    print("VALIDAÇÃO 1: Sintaxe YAML")
    print("=" * 70)
    
    parsed_files = {}
    all_valid = True
    
    for filepath in COMPOSE_FILES:
        filename = os.path.basename(filepath)
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = yaml.safe_load(f)
            
            if data is None:
                print(f"  ⚠️  {filename}: arquivo vazio ou apenas comentários")
                all_valid = False
            elif not isinstance(data, dict):
                print(f"  ❌ {filename}: conteúdo YAML não é um mapeamento válido")
                all_valid = False
            else:
                services_count = len(data.get('services', {}))
                print(f"  ✅ {filename}: YAML válido ({services_count} serviços)")
                parsed_files[filename] = data
                
        except yaml.YAMLError as e:
            print(f"  ❌ {filename}: Erro de sintaxe YAML!")
            print(f"     Detalhes: {e}")
            all_valid = False
        except FileNotFoundError:
            print(f"  ❌ {filename}: Arquivo não encontrado!")
            all_valid = False
    
    print()
    return all_valid, parsed_files


def validate_images(parsed_files):
    """Validação 2: Verificar que todas as imagens referenciadas são as esperadas."""
    print("=" * 70)
    print("VALIDAÇÃO 2: Imagens Docker referenciadas")
    print("=" * 70)
    
    all_images = set()
    for filename, data in parsed_files.items():
        services = data.get('services', {})
        for svc_name, svc_config in services.items():
            image = svc_config.get('image', '')
            if image:
                all_images.add(image)
    
    print(f"  Imagens encontradas nos compose files:")
    for img in sorted(all_images):
        status = "✅" if img in EXPECTED_IMAGES else "⚠️ "
        print(f"    {status} {img}")
    
    print(f"\n  Imagens esperadas pelo design:")
    all_present = True
    for img in EXPECTED_IMAGES:
        if img in all_images:
            print(f"    ✅ {img} - presente")
        else:
            print(f"    ❌ {img} - AUSENTE")
            all_present = False
    
    print()
    return all_present


def validate_ports(parsed_files):
    """Validação 3: Verificar conflitos de porta entre serviços."""
    print("=" * 70)
    print("VALIDAÇÃO 3: Conflitos de porta")
    print("=" * 70)
    
    no_conflicts = True
    
    # Verificar portas dentro de cada arquivo individualmente
    for filename, data in parsed_files.items():
        print(f"\n  📄 {filename}:")
        services = data.get('services', {})
        port_map = {}  # host_port -> (service_name, mapping)
        
        for svc_name, svc_config in services.items():
            ports = svc_config.get('ports', [])
            for port_mapping in ports:
                port_str = str(port_mapping)
                # Parse "host:container" format
                parts = port_str.split(':')
                if len(parts) == 2:
                    host_port = parts[0].strip('"').strip("'")
                elif len(parts) == 3:
                    host_port = parts[1]  # IP:host:container
                else:
                    host_port = parts[0]
                
                try:
                    host_port_int = int(host_port)
                except ValueError:
                    continue
                
                if host_port_int in port_map:
                    conflict_svc, conflict_mapping = port_map[host_port_int]
                    print(f"    ❌ CONFLITO: Porta {host_port_int} usada por "
                          f"'{svc_name}' e '{conflict_svc}'")
                    no_conflicts = False
                else:
                    port_map[host_port_int] = (svc_name, port_str)
                    print(f"    ✅ Porta {host_port_int} → {svc_name}")
    
    # Verificar portas esperadas no full stack
    print(f"\n  Portas esperadas (Requisito 4.6):")
    if "docker-compose.full.yml" in parsed_files:
        full_data = parsed_files["docker-compose.full.yml"]
        full_services = full_data.get('services', {})
        full_ports = set()
        for svc_name, svc_config in full_services.items():
            for port_mapping in svc_config.get('ports', []):
                parts = str(port_mapping).split(':')
                if len(parts) >= 2:
                    try:
                        full_ports.add(int(parts[0].strip('"').strip("'")))
                    except ValueError:
                        pass
        
        for name, port in EXPECTED_PORTS.items():
            if port in full_ports:
                print(f"    ✅ {name}: porta {port} - configurada")
            else:
                print(f"    ❌ {name}: porta {port} - NÃO ENCONTRADA")
                no_conflicts = False
    
    print()
    return no_conflicts


def validate_networks(parsed_files):
    """Validação 4: Verificar consistência de configuração de rede."""
    print("=" * 70)
    print("VALIDAÇÃO 4: Consistência de rede")
    print("=" * 70)
    
    consistent = True
    network_configs = {}
    
    for filename, data in parsed_files.items():
        networks = data.get('networks', {})
        services = data.get('services', {})
        
        print(f"\n  📄 {filename}:")
        
        # Redes definidas no top-level
        for net_name, net_config in networks.items():
            if net_config is None:
                net_config = {}
            is_external = net_config.get('external', False) if isinstance(net_config, dict) else False
            driver = net_config.get('driver', 'default') if isinstance(net_config, dict) else 'default'
            real_name = net_config.get('name', net_name) if isinstance(net_config, dict) else net_name
            
            print(f"    Rede '{net_name}': driver={driver}, external={is_external}, name={real_name}")
            network_configs[filename] = {
                'name': real_name,
                'external': is_external,
                'driver': driver,
            }
        
        # Verificar que todos os serviços usam a rede
        services_without_network = []
        for svc_name, svc_config in services.items():
            svc_networks = svc_config.get('networks', [])
            if not svc_networks:
                services_without_network.append(svc_name)
        
        if services_without_network:
            print(f"    ⚠️  Serviços sem rede explícita: {services_without_network}")
        else:
            print(f"    ✅ Todos os serviços conectados à rede")
    
    # Verificar que todos usam a mesma rede
    network_names = set()
    for filename, config in network_configs.items():
        network_names.add(config['name'])
    
    if len(network_names) == 1:
        print(f"\n  ✅ Todos os arquivos usam a mesma rede: '{network_names.pop()}'")
    elif len(network_names) > 1:
        print(f"\n  ❌ Redes diferentes detectadas: {network_names}")
        consistent = False
    
    # Verificar compatibilidade external/bridge
    base_is_bridge = False
    override_is_external = False
    for filename, config in network_configs.items():
        if filename == "docker-compose.yml":
            base_is_bridge = config['driver'] == 'bridge'
        elif filename == "docker-compose.airflow.yml":
            override_is_external = config['external']
    
    if base_is_bridge and override_is_external:
        print(f"  ✅ Rede base é bridge, override marca como external - compatível")
    
    print()
    return consistent


def check_docker_available():
    """Tenta verificar se Docker está disponível."""
    print("=" * 70)
    print("VALIDAÇÃO 5: Docker Compose config (se disponível)")
    print("=" * 70)
    
    import subprocess
    try:
        result = subprocess.run(
            ["docker", "compose", "version"],
            capture_output=True, text=True, timeout=10
        )
        if result.returncode == 0:
            print(f"  ✅ Docker Compose disponível: {result.stdout.strip()}")
            # Tentar validar o compose file
            result = subprocess.run(
                ["docker", "compose", "-f", 
                 os.path.join(SCRIPT_DIR, "docker-compose.yml"), "config"],
                capture_output=True, text=True, timeout=30
            )
            if result.returncode == 0:
                print(f"  ✅ docker-compose.yml: configuração válida")
            else:
                print(f"  ⚠️  docker-compose.yml: {result.stderr.strip()}")
            return True
        else:
            print(f"  ⚠️  Docker não está rodando ou não disponível")
            print(f"     Erro: {result.stderr.strip()}")
            return False
    except FileNotFoundError:
        print(f"  ⚠️  Docker não está instalado neste ambiente")
        return False
    except subprocess.TimeoutExpired:
        print(f"  ⚠️  Timeout ao tentar verificar Docker")
        return False
    except Exception as e:
        print(f"  ⚠️  Erro ao verificar Docker: {e}")
        return False


def main():
    print()
    print("🐳 VALIDAÇÃO DO AMBIENTE DOCKER COMPOSE")
    print("   Curso: Big Data Processing - Mackenzie MBA")
    print("=" * 70)
    print()
    
    results = {}
    
    # 1. Validar YAML
    yaml_valid, parsed_files = validate_yaml_syntax()
    results['yaml'] = yaml_valid
    
    if not parsed_files:
        print("❌ Não foi possível continuar sem arquivos YAML válidos.")
        sys.exit(1)
    
    # 2. Validar imagens
    results['images'] = validate_images(parsed_files)
    
    # 3. Validar portas
    results['ports'] = validate_ports(parsed_files)
    
    # 4. Validar redes
    results['networks'] = validate_networks(parsed_files)
    
    # 5. Docker (se disponível)
    results['docker'] = check_docker_available()
    
    # Resumo
    print()
    print("=" * 70)
    print("📋 RESUMO DA VALIDAÇÃO")
    print("=" * 70)
    print(f"  {'✅' if results['yaml'] else '❌'} Sintaxe YAML: {'válida' if results['yaml'] else 'INVÁLIDA'}")
    print(f"  {'✅' if results['images'] else '❌'} Imagens Docker: {'corretas' if results['images'] else 'PROBLEMAS'}")
    print(f"  {'✅' if results['ports'] else '❌'} Portas: {'sem conflitos' if results['ports'] else 'CONFLITOS'}")
    print(f"  {'✅' if results['networks'] else '❌'} Redes: {'consistentes' if results['networks'] else 'INCONSISTENTES'}")
    print(f"  {'✅' if results['docker'] else '⚠️ '} Docker: {'validado' if results['docker'] else 'não disponível (apenas validação estática)'}")
    print()
    
    # Status final
    core_valid = all([results['yaml'], results['images'], results['ports'], results['networks']])
    if core_valid:
        print("🎉 RESULTADO: Todos os arquivos Docker Compose estão corretamente configurados!")
        print("   O ambiente pode ser iniciado com 'docker compose up -d'")
    else:
        print("❌ RESULTADO: Problemas encontrados. Verifique os detalhes acima.")
    
    print()
    return 0 if core_valid else 1


if __name__ == "__main__":
    sys.exit(main())
